// Package runner is the single place that turns "run workflow X
// starting at node Y, in mode Z, with seed input W" into an
// engine.Run call. Previously this logic was only wired up for the
// manual-execute HTTP endpoint, with scheduler/webhook triggers left
// as TODOs that merely logged; this package removes that duplication
// so all three trigger paths behave identically (same scratch-dir
// handling, same execution persistence, same cleanup).
package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

type WorkflowLoader interface {
	LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error)
}

type ExecutionSaver interface {
	SaveExecution(ctx context.Context, ex *model.Execution) error
}

type Runner struct {
	Workflows   WorkflowLoader
	Execs       ExecutionSaver
	Engine      *engine.Engine
	StaticData  engine.StaticDataStore
	Credentials engine.CredentialResolver
	ScratchRoot string
	// Timeout bounds one full workflow run (rule 22: nothing unbounded).
	Timeout time.Duration
	// MemGuard, if set, is attached to every RunContext this Runner
	// creates so node executors (HTTP/command/code) can back off new
	// heavy work while the process is over its soft RAM ceiling.
	MemGuard *engine.MemGuard
	// NodeRunCap is attached to every RunContext this Runner creates
	// (see engine.RunContext.NodeRunCap). Zero/unset falls back to the
	// engine's own 500 default; set low via WithNodeRunCap for a
	// tight-RAM single-workflow deployment.
	NodeRunCap int
	// sem, if non-nil, bounds how many engine.Run executions may be in
	// flight at once *across every trigger path* -- manual/async via
	// Manager, and direct RunFromNode calls from the scheduler and
	// webhook server. Set via WithConcurrencyLimit, or implicitly
	// shared in by NewManager if it wasn't set first. nil (the
	// zero value) means unbounded, which only bare Runners built
	// without a Manager (e.g. in tests) should ever run with in a
	// real deployment.
	sem chan struct{}
}

// WithConcurrencyLimit creates the Runner's shared execution
// semaphore, bounding how many workflow runs -- regardless of trigger
// type -- may execute at once. This closes a low-RAM-deployment gap:
// MICROFLOW_MAX_CONCURRENT_EXECUTIONS previously only gated the async
// HTTP execute path (Manager's own private semaphore), so a webhook
// and a schedule (or two webhooks) firing at the same moment could
// each start a full workflow run -- including FFmpeg/TTS/JS-VM work --
// in parallel, with nothing to stop it. Call this before constructing
// a Manager on top of the same Runner; NewManager will reuse this
// semaphore instead of making its own if one is already set.
func (r *Runner) WithConcurrencyLimit(maxConcurrent int) *Runner {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	r.sem = make(chan struct{}, maxConcurrent)
	return r
}

func New(workflows WorkflowLoader, execs ExecutionSaver, eng *engine.Engine, staticData engine.StaticDataStore, creds engine.CredentialResolver, scratchRoot string) *Runner {
	return &Runner{
		Workflows:   workflows,
		Execs:       execs,
		Engine:      eng,
		StaticData:  staticData,
		Credentials: creds,
		ScratchRoot: scratchRoot,
		Timeout:     30 * time.Minute,
	}
}

// WithTimeout overrides the default 30-minute Timeout. Exposed as a
// setter (rather than requiring callers to poke the field directly)
// so cmd/server can wire it from an env var the same way it wires
// MemGuard/NodeRunCap -- a 30-minute flat cap is far too short for a
// large real-world pipeline (AI generation + TTS + FFmpeg rendering
// across 100+ nodes can easily run well past 30 minutes end to end),
// and the right number depends on the operator's own workflow, not on
// this package.
func (r *Runner) WithTimeout(d time.Duration) *Runner {
	if d > 0 {
		r.Timeout = d
	}
	return r
}

// WithMemGuard attaches a MemGuard so every future RunFromNode call's
// RunContext carries it. Optional -- a Runner with no guard behaves
// exactly as before (nil MemGuard is a documented no-op).
func (r *Runner) WithMemGuard(g *engine.MemGuard) *Runner {
	r.MemGuard = g
	return r
}

// WithNodeRunCap sets the per-run in-memory execution-history cap (see
// engine.RunContext.NodeRunCap's doc comment) attached to every future
// RunFromNode call's RunContext. Optional -- zero/unset behaves exactly
// as before (falls back to the engine's own 500 default).
func (r *Runner) WithNodeRunCap(n int) *Runner {
	r.NodeRunCap = n
	return r
}

// RunFromNode loads workflowID, starts execution at startNode with the
// given mode ("manual" | "schedule" | "webhook" | "error") and seed
// input, persists the resulting execution record (win or lose), and
// cleans up the run's scratch directory afterward (rule 7/9). It
// blocks until the run finishes -- the scheduler and webhook trigger
// paths want exactly that. The async HTTP execute endpoint instead
// uses Manager (async.go), which wraps runOnce directly so it can
// return before the run completes.
func (r *Runner) RunFromNode(ctx context.Context, workflowID, startNode, mode string, seed model.NodeOutput) (*model.Execution, error) {
	wf, err := r.Workflows.LoadWorkflow(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("runner: load workflow %q: %w", workflowID, err)
	}
	if _, ok := wf.Nodes[startNode]; !ok {
		return nil, fmt.Errorf("runner: workflow %q has no node named %q", workflowID, startNode)
	}

	// Bound this run by the shared execution semaphore (see
	// WithConcurrencyLimit's doc comment) so scheduler/webhook runs
	// count against the same MICROFLOW_MAX_CONCURRENT_EXECUTIONS
	// limit as the async HTTP path, instead of bypassing it entirely.
	// A Runner with no semaphore configured (r.sem == nil) runs
	// unbounded, same as before this fix -- only bare Runners built
	// without ever calling WithConcurrencyLimit/NewManager hit this.
	//
	// Bug fix: this wait used to happen *inside* the r.Timeout window
	// (the timeout context was created first, then waited on for the
	// semaphore). With a low-RAM deployment's typical
	// MICROFLOW_MAX_CONCURRENT_EXECUTIONS=1, a run queued behind an
	// already-running execution burned its entire Timeout budget just
	// sitting in line, then had almost nothing left once it actually
	// started -- observed in practice as a run getting cancelled with
	// "context deadline exceeded" a couple of minutes after its first
	// node started, even though the run itself was healthy. The
	// semaphore wait now uses the caller's own ctx (no extra
	// deadline); r.Timeout's clock only starts once a worker slot is
	// actually acquired, i.e. once the run can really begin.
	if r.sem != nil {
		select {
		case r.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, fmt.Errorf("runner: %w waiting for a free execution slot", ctx.Err())
		}
		defer func() { <-r.sem }()
	}

	runCtx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	return r.runOnce(runCtx, wf, newID(), startNode, mode, seed, nil)
}

// runOnce is the single place that actually builds a RunContext, calls
// engine.Run, persists the result, and cleans up scratch -- shared by
// the synchronous RunFromNode above and the async Manager (async.go).
// execID is supplied by the caller (Manager pre-allocates it so it can
// hand the id back to the HTTP client before the run starts).
// onNodeRun, if non-nil, is wired to RunContext.OnNodeRun for live
// per-node progress (SSE); RunFromNode's callers (scheduler/webhook)
// don't need it and pass nil.
func (r *Runner) runOnce(ctx context.Context, wf *model.Workflow, execID, startNode, mode string, seed model.NodeOutput, onNodeRun func(model.NodeRunResult)) (*model.Execution, error) {
	scratchDir := filepath.Join(r.ScratchRoot, execID)
	// Bug fix: this per-execution directory was never actually created --
	// it was only ever used as exec.Cmd.Dir for executeCommand nodes
	// (FFmpeg/TTS), which failed immediately with a chdir error on every
	// single run (then got misreported as a timeout by the bug fixed in
	// nodes/command.go). spoolBinary's own os.MkdirAll calls masked this
	// for HTTP-response/file-write nodes, which is why it went unnoticed.
	if err := os.MkdirAll(scratchDir, 0o700); err != nil {
		return nil, fmt.Errorf("runner: create scratch dir for execution %q: %w", execID, err)
	}

	rc := &engine.RunContext{
		Workflow:    wf,
		Execution:   &model.Execution{ID: execID, WorkflowID: wf.ID, Mode: mode, Status: model.StatusRunning},
		StaticData:  r.StaticData,
		Credentials: r.Credentials,
		ScratchDir:  scratchDir,
		MemGuard:    r.MemGuard,
		// Seeded fresh per run from the current process environment (not
		// cached on Runner) so a rotated secret takes effect immediately
		// and every run's redaction reflects the credentials actually in
		// use for it. Never touches the live values node executors use --
		// see RunContext.Redactor's doc comment.
		Redactor:   engine.NewSecretRedactorFromEnv(),
		OnNodeRun:  onNodeRun,
		NodeRunCap: r.NodeRunCap,
	}

	ex, runErr := r.Engine.Run(ctx, rc, startNode, seed)

	// Always persist, even on failure -- the execution panel/history
	// needs the error/status either way (rule: execution panel shows
	// status/input/output/error/logs/duration regardless of outcome).
	if saveErr := r.Execs.SaveExecution(context.Background(), ex); saveErr != nil {
		log.Printf("runner: failed to persist execution %s: %v", execID, saveErr)
	}
	cleanupScratch(scratchDir)

	return ex, runErr
}

// FirstTriggerNode picks a start node when the caller doesn't specify
// one (manual "Execute" button with nothing selected).
func FirstTriggerNode(wf *model.Workflow) string {
	for name, n := range wf.Nodes {
		switch n.Type {
		case model.TypeManualTrigger, model.TypeScheduleTrigger, model.TypeWebhookTrigger:
			return name
		}
	}
	return ""
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func cleanupScratch(dir string) {
	go func() {
		_ = os.RemoveAll(dir)
	}()
}
