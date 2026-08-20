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
}

// appendNodeRun records one node's result, trimming the oldest entries
// once nodeRunCap is exceeded (default 500, matching the store's
// persisted-history cap) instead of growing rc.Execution.NodeRuns
// without bound for the lifetime of a long-running execution.
func (rc *RunContext) appendNodeRun(result model.NodeRunResult) {
	limit := rc.nodeRunCap
	if limit <= 0 {
		limit = 500
	}
	rc.Execution.NodeRuns = append(rc.Execution.NodeRuns, result)
	if extra := len(rc.Execution.NodeRuns) - limit; extra > 0 {
		rc.Execution.NodeRuns = rc.Execution.NodeRuns[extra:]
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
			if a < maxTries {
				time.Sleep(time.Duration(node.WaitBetweenTriesMs) * time.Millisecond)
			}
		}
		duration := time.Since(started)

		result := model.NodeRunResult{
			NodeName:  node.Name,
			Input:     item.input,
			StartedAt: started,
			Duration:  duration,
			Attempt:   attempt,
		}

		if nodeErr != nil {
			result.Status = model.StatusError
			result.Error = nodeErr.Error()
			rc.appendNodeRun(result)
			if node.ContinueOnFail {
				continue // swallow and keep processing the rest of the queue
			}
			runErr = fmt.Errorf("node %q failed: %w", node.Name, nodeErr)
			rc.Execution.Status = model.StatusError
			break runLoop
		}

		result.Status = model.StatusSuccess
		result.Output = out
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
