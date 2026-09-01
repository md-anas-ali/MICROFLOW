// Package engine is MicroFlow's execution engine. It is a dataflow
// work-queue interpreter, not a strict topological-sort DAG runner --
// that choice is deliberate: the sample workflow contains genuine
// cycles (e.g. "Retry New Topic (loop back)" -> NoOp -> back upstream,
// and every Model-Controller/Validate/Router/Wait retry loop). A
// work-queue naturally supports that without special-casing "loop
// nodes"; a topo-sort engine would not.
package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"microflow/internal/expr"
	"microflow/internal/model"
)

// NodeExecutor is implemented once per model.NodeType (see internal/nodes).
type NodeExecutor interface {
	Execute(ctx context.Context, rc *RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error)
}

// StaticDataStore backs n8n's $getWorkflowStaticData('global') equivalent.
// Implementations MUST make Get+mutate+Set safe under concurrent runs of
// the same workflow (row-level locking in the Postgres implementation --
// see internal/store) since the spec explicitly calls out race
// conditions on shared counters/dedup state/retry queues.
type StaticDataStore interface {
	// WithLock runs fn while holding an exclusive lock on the workflow's
	// static data row, passing the current data and expecting the
	// (possibly mutated) data back to persist.
	WithLock(ctx context.Context, workflowID string, fn func(data map[string]any) (map[string]any, error)) error
}

// CredentialResolver looks up a decrypted secret for (workflowID, logicalName).
// Never logs, never returns secrets in error messages.
type CredentialResolver interface {
	Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error)
}

// RunContext is passed to every node executor: it carries the workflow,
// the live execution log, the static-data/credential backends, a
// per-run scratch directory for spooled binaries, and cancellation.
type RunContext struct {
	Workflow    *model.Workflow
	Execution   *model.Execution
	StaticData  StaticDataStore
	Credentials CredentialResolver
	ScratchDir  string // per-execution temp dir; engine removes it on completion (rule 7: cleanup)
	mu          sync.Mutex
	nodeOutputs map[string]map[string]any // last output's $json per node name, for $node[...] expressions

	// resourceGate bounds concurrent heavy work (FFmpeg/TTS/HTTP) so a
	// single execution never spins up unbounded child processes -- rule
	// 20 (no unbounded concurrency) and rule 19 (RAM-aware throttling).
	HeavyWorkGate chan struct{}

	// MemGuard, if set, lets node executors back off (via
	// MemGuard.WaitIfThrottled) before starting new heavy work when the
	// process is already near its RAM ceiling. Optional: nil is safe
	// (WaitIfThrottled/ShouldThrottle no-op on a nil receiver), so
	// callers/tests that don't wire one still work unchanged.
	MemGuard *MemGuard

	// maxNodeRunsInMemory bounds how many NodeRunResult entries are kept
	// live in rc.Execution.NodeRuns during a run, so a workflow with a
	// long retry/loop pattern (up to MaxSteps steps) never accumulates
	// thousands of full input/output JSON snapshots in RAM before the
	// DB-side truncation in internal/store even gets a chance to run
	// (rule 18: don't let logs/JSON pile up in memory). Older entries are
	// dropped once the cap is exceeded; the most recent entries (which is
	// what an error/status needs) are always kept.
	nodeRunCap int

	// Redactor scrubs secret-shaped values (API keys, tokens, passwords
	// -- see redact.go) out of anything stored in Execution.NodeRuns,
	// which is what gets persisted, returned from the execute API, and
	// rendered in the frontend's node inspector. It never touches the
	// live data node executors/expressions actually work with (see
	// recordNodeOutput below), so Config Center's real key values keep
	// flowing to the nodes that need them -- only the copy that gets
	// displayed/logged is redacted. Nil is a safe no-op (a Runner that
	// doesn't set one behaves exactly as before, just unredacted).
	Redactor *SecretRedactor

	// OnNodeRun, if set, is invoked synchronously with every
	// NodeRunResult immediately after it's appended to
	// Execution.NodeRuns -- the single hook point live-progress delivery
	// (SSE) is built on. It MUST return quickly and MUST NOT block on
	// I/O: a slow/blocked subscriber would otherwise stall the engine
	// itself. internal/runner's async Manager wires this to a bounded,
	// non-blocking per-execution broadcaster (drop-oldest under
	// backpressure), never a direct network write. Nil is a safe no-op.
	OnNodeRun func(model.NodeRunResult)
}

// appendNodeRun records one node's result, trimming the oldest entries
// once nodeRunCap is exceeded (default 500, matching the store's
// persisted-history cap) instead of growing rc.Execution.NodeRuns
// without bound for the lifetime of a long-running execution. Also
// fires OnNodeRun (if set) so live progress (SSE) reflects every node
// transition, not just the terminal execution state.
func (rc *RunContext) appendNodeRun(result model.NodeRunResult) {
	limit := rc.nodeRunCap
	if limit <= 0 {
		limit = 500
	}
	rc.Execution.NodeRuns = append(rc.Execution.NodeRuns, result)
	if extra := len(rc.Execution.NodeRuns) - limit; extra > 0 {
		rc.Execution.NodeRuns = rc.Execution.NodeRuns[extra:]
	}
	if rc.OnNodeRun != nil {
		rc.OnNodeRun(result)
	}
}

func (rc *RunContext) recordNodeOutput(name string, out model.NodeOutput) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(out) == 0 || len(out[0]) == 0 {
		return
	}
	if rc.nodeOutputs == nil {
		rc.nodeOutputs = map[string]map[string]any{}
	}
	rc.nodeOutputs[name] = out[0][0].JSON
}

func (rc *RunContext) ExprContext(currentJSON map[string]any) expr.Context {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return expr.Context{
		JSON:        currentJSON,
		NodeOutputs: rc.nodeOutputs,
		Execution: map[string]any{
			"id":   rc.Execution.ID,
			"mode": rc.Execution.Mode,
		},
	}
}

// Engine ties a node-type registry to workflow execution.
type Engine struct {
	Registry map[model.NodeType]NodeExecutor

	// MaxSteps hard-caps total node executions per run, independent of
	// any individual node's retry count -- a backstop against runaway
	// loops (security rule 22: "Infinite loops").
	MaxSteps int
	// MaxConcurrentHeavy bounds simultaneous executeCommand/httpRequest
	// work (FFmpeg, TTS, AI calls) for the 512MB RAM budget.
	MaxConcurrentHeavy int
}

func New(registry map[model.NodeType]NodeExecutor) *Engine {
	return &Engine{
		Registry:           registry,
		MaxSteps:           5000,
		MaxConcurrentHeavy: 2,
	}
}

type queueItem struct {
	nodeName string
	input    model.NodeOutput
}

// Run executes wf starting from the given trigger node with the given
// seed input, following connections as a work queue. It returns a
// fully populated model.Execution (for the Execution panel: status,
// per-node input/output/error/logs/duration) whether or not the run
// ultimately errors.
func (e *Engine) Run(ctx context.Context, rc *RunContext, startNode string, seed model.NodeOutput) (*model.Execution, error) {
	rc.Execution.Status = model.StatusRunning
	rc.Execution.StartedAt = time.Now()
	rc.HeavyWorkGate = make(chan struct{}, e.MaxConcurrentHeavy)

	queue := []queueItem{{nodeName: startNode, input: seed}}
	steps := 0
	// perNodeAttempts survives across the whole run so a node's own
	// RetryOnFail/MaxTries budget (not the workflow's manual retry-loop
	// pattern, which is just ordinary graph traversal) is enforced.
	perNodeAttempts := map[string]int{}

	var runErr error

runLoop:
	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			rc.Execution.Status = model.StatusCancelled
			runErr = ctx.Err()
			break runLoop
		default:
		}

		steps++
		if steps > e.MaxSteps {
			runErr = fmt.Errorf("engine: exceeded MaxSteps (%d) -- probable infinite loop, aborting", e.MaxSteps)
			rc.Execution.Status = model.StatusError
			break runLoop
		}

		item := queue[0]
		queue = queue[1:]

		node, ok := rc.Workflow.Nodes[item.nodeName]
		if !ok {
			log.Printf("engine: connection references unknown node %q, skipping", item.nodeName)
			continue
		}
		if node.Disabled || node.Type == model.TypeStickyNote {
			continue
		}
		exec, ok := e.Registry[node.Type]
		if !ok {
			runErr = fmt.Errorf("engine: no executor registered for node %q (type %q) -- unsupported node must not be silently skipped", node.Name, node.OriginalType)
			rc.Execution.Status = model.StatusError
			break runLoop
		}

		perNodeAttempts[node.Name]++
		attempt := perNodeAttempts[node.Name]
		maxTries := 1
		if node.RetryOnFail && node.MaxTries > 1 {
			maxTries = node.MaxTries
		}

		started := time.Now()
		var out model.NodeOutput
		var nodeErr error
		for a := 1; a <= maxTries; a++ {
			out, nodeErr = exec.Execute(ctx, rc, node, item.input)
			if nodeErr == nil {
				break
			}
			// A permanent (config/credential) error can't be fixed by
			// retrying the identical request again -- e.g. an HTTP
			// 401/403 from a missing/invalid API key. Stop immediately
			// instead of burning the rest of the MaxTries budget (and
			// their backoff waits) on attempts guaranteed to fail the
			// same way; this also makes the resulting error message
			// arrive faster and stay unambiguous (transient vs
			// permanent), per the "clear configuration error" goal.
			if IsPermanent(nodeErr) {
				break
			}
			if a < maxTries {
				time.Sleep(retryBackoff(node.WaitBetweenTriesMs, a))
			}
		}
		duration := time.Since(started)

		result := model.NodeRunResult{
			NodeName:  node.Name,
			Input:     rc.Redactor.RedactOutput(item.input),
			StartedAt: started,
			Duration:  duration,
			Attempt:   attempt,
		}

		if nodeErr != nil {
			if ctx.Err() != nil {
				// The node failed because the run itself was
				// cancelled/timed out (its Execute call got ctx.Err()
				// back), not because of an ordinary node-level error --
				// report the correct terminal state instead of
				// StatusError, and don't apply continueOnFail/onError
				// (those are for real business-logic failures, not "the
				// whole run stopped"). Rule H/P: cancellation must
				// produce the correct terminal state, not be
				// misreported.
				result.Status = model.StatusCancelled
				result.Error = rc.Redactor.RedactString(nodeErr.Error())
				rc.appendNodeRun(result)
				rc.Execution.Status = model.StatusCancelled
				runErr = ctx.Err()
				break runLoop
			}
			result.Status = model.StatusError
			result.Error = rc.Redactor.RedactString(nodeErr.Error())
			if node.ContinueOnFail {
				// Match n8n's actual continueOnFail/onError behavior:
				// the failure doesn't just vanish -- it becomes a
				// pass-through item carrying the error, which flows on
				// (a) the node's normal output, mixed in with regular
				// items, for onError=continueRegularOutput (or the
				// legacy bare continueOnFail:true), or (b) the node's
				// dedicated second/error output branch, for
				// onError=continueErrorOutput. Previously this branch
				// just swallowed the error and stopped -- nothing
				// flowed downstream at all, which silently truncated
				// any workflow relying on a real error/fallback branch
				// (exactly the "silent failure" class of bug flagged
				// for fix) even though the run reported StatusSuccess.
				errItem := model.Item{JSON: map[string]any{"error": nodeErr.Error()}}
				errOut := model.NodeOutput{{errItem}}
				branchIdx := 0
				if node.OnErrorMode == "continueErrorOutput" {
					branchIdx = 1
					errOut = model.NodeOutput{{}, {errItem}}
				}
				result.Output = rc.Redactor.RedactOutput(errOut)
				rc.appendNodeRun(result)
				enqueueFromBranch(&queue, rc.Workflow, node.Name, branchIdx, []model.Item{errItem})
				continue
			}
			rc.appendNodeRun(result)
			runErr = fmt.Errorf("node %q failed: %w", node.Name, nodeErr)
			rc.Execution.Status = model.StatusError
			break runLoop
		}

		result.Status = model.StatusSuccess
		result.Output = rc.Redactor.RedactOutput(out)
		rc.appendNodeRun(result)
		rc.recordNodeOutput(node.Name, out)

		for _, conn := range rc.Workflow.Connections[node.Name] {
			if conn.SourceIndex >= len(out) {
				continue
			}
			branchItems := out[conn.SourceIndex]
			if len(branchItems) == 0 {
				continue
			}
			queue = append(queue, queueItem{
				nodeName: conn.TargetName,
				input:    model.NodeOutput{branchItems},
			})
		}
	}

	if runErr == nil && rc.Execution.Status == model.StatusRunning {
		rc.Execution.Status = model.StatusSuccess
	}
	now := time.Now()
	rc.Execution.FinishedAt = &now
	if runErr != nil {
		rc.Execution.Error = runErr.Error()
	}
	return rc.Execution, runErr
}

// enqueueFromBranch appends one queue item per connection leaving
// node.Name on output index branchIdx, carrying items as that
// connection's input. Used both by the normal success path (inlined
// above) and by the ContinueOnFail error-item path, which needs the
// exact same branch-routing logic (continueErrorOutput routes to
// index 1, continueRegularOutput/legacy continueOnFail to index 0).
func enqueueFromBranch(queue *[]queueItem, wf *model.Workflow, nodeName string, branchIdx int, items []model.Item) int {
	if len(items) == 0 {
		return 0
	}
	n := 0
	for _, conn := range wf.Connections[nodeName] {
		if conn.SourceIndex != branchIdx {
			continue
		}
		*queue = append(*queue, queueItem{nodeName: conn.TargetName, input: model.NodeOutput{items}})
		n++
	}
	return n
}

// maxRetryBackoff caps exponential backoff between a node's own retry
// attempts so a misconfigured/very large WaitBetweenTriesMs (or a
// high MaxTries) can never stall a run for an unreasonable time.
const maxRetryBackoff = 30 * time.Second

// retryBackoff computes the wait before retry attempt (a+1), given the
// node's configured base wait in milliseconds. It doubles per attempt
// (1x, 2x, 4x, ...) so a run of transient 429/5xx errors backs off
// instead of hammering the same failing endpoint on a fixed interval,
// capped at maxRetryBackoff. baseMs<=0 falls back to a 1s base (the
// parser already defaults WaitBetweenTriesMs to 1000 when unset, so
// this is just a defensive floor for callers that construct a Node
// directly, e.g. in tests).
func retryBackoff(baseMs int, attempt int) time.Duration {
	if baseMs <= 0 {
		baseMs = 1000
	}
	base := time.Duration(baseMs) * time.Millisecond
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 10 { // guard against overflow from a runaway attempt count
		shift = 10
	}
	wait := base << shift
	if wait > maxRetryBackoff || wait <= 0 {
		wait = maxRetryBackoff
	}
	return wait
}
