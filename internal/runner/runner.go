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

// WithMemGuard attaches a MemGuard so every future RunFromNode call's
// RunContext carries it. Optional -- a Runner with no guard behaves
// exactly as before (nil MemGuard is a documented no-op).
func (r *Runner) WithMemGuard(g *engine.MemGuard) *Runner {
	r.MemGuard = g
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
		Redactor:  engine.NewSecretRedactorFromEnv(),
		OnNodeRun: onNodeRun,
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
