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
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"microflow/internal/autoopt"
	"microflow/internal/engine"
	"microflow/internal/model"
)

type WorkflowLoader interface {
	LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error)
}

type ExecutionSaver interface {
	SaveExecution(ctx context.Context, ex *model.Execution) error
}

// ExecutionDeleter is implemented by durable stores that should discard the
// terminal execution row immediately. This is intentionally separate from
// ExecutionSaver so lightweight test/live implementations do not need a
// database-specific cleanup method.
type ExecutionDeleter interface {
	DeleteExecution(ctx context.Context, executionID string) error
}

// EnvResolver decrypts and returns one run's Global/Service Environment
// maps (see vault.EnvVault.ResolveAll). Optional: a Runner with no
// EnvResolver leaves RunContext.ServiceEnv/GlobalEnv nil, so
// RunContext.Env/LookupEnv fall straight through to the process
// environment -- identical to every pre-Service-isolation behavior
// (rule 14), and every existing test helper that builds a bare Runner
// keeps working unchanged.
type EnvResolver interface {
	ResolveAll(ctx context.Context, serviceID string) (serviceEnv, globalEnv map[string]string, err error)
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
	// Recovery is the durable PostgreSQL-backed checkpoint store. Optional for tests.
	Recovery CheckpointStore

	// AutoOpt, if set, gives every execution an ownership record
	// (autoopt.ServiceContext) that is released in runOnce's deferred
	// teardown no matter how the run ends. Optional; nil = pre-Auto-Optimize
	// behaviour exactly.
	AutoOpt *autoopt.Optimizer

	// Env resolves each run's Global/Service Environment (rule 2),
	// looked up once per run by the workflow's owning ServiceID and
	// attached to RunContext -- see runOnce. Optional; nil preserves
	// pre-Service-isolation behavior (process environment only).
	Env EnvResolver

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

	// activeMu/activeRuns is a small in-process refcount of workflow
	// IDs with a run currently in flight through this Runner --
	// covers RunFromNode's whole call (scheduler/webhook, synchronous)
	// and Manager's whole queued-through-finished lifecycle (async
	// HTTP execute), not just the time engine.Run is actually
	// executing. It exists solely so the workflow-delete endpoint can
	// ask IsWorkflowActive before deleting (rule 13: never delete a
	// workflow out from under a running execution). Deliberately not a
	// new dependency/cache/worker -- just a mutex-guarded map, same
	// footprint class as the maps already used throughout this
	// package and internal/scheduler.
	activeMu   sync.Mutex
	activeRuns map[string]int

	// liveExecMu/liveExecs is the in-process set of execution IDs that
	// runOnce is executing right now, for EVERY trigger path (async
	// Manager, scheduler, webhook, recovered runs). Manager.states only
	// knows about async/recovered runs, so without this the recovery loop
	// cannot tell a live scheduler/webhook run from a crashed one (both
	// have a non-terminal executions row and no checkpoint lease) and would
	// resume or fail-and-delete an execution that is still running.
	liveExecMu sync.Mutex
	liveExecs  map[string]struct{}

	// scratchWG counts scratch-directory removals still in flight (they are
	// deleted in the background so a run's result is never delayed), so the
	// global scheduler's between-run cleanup can wait for them (see
	// WaitScratchCleanup) before the next Service starts.
	scratchWG sync.WaitGroup

	// tmpMu/tmpBaseline: entries that already existed directly under the OS
	// temp dir when the server started (see leftovers.go). Anything that
	// appears there afterwards is leftover junk from a finished run.
	tmpMu       sync.Mutex
	tmpBaseline map[string]struct{}
}

func (r *Runner) markExecutionLive(execID string) {
	r.liveExecMu.Lock()
	if r.liveExecs == nil {
		r.liveExecs = map[string]struct{}{}
	}
	r.liveExecs[execID] = struct{}{}
	r.liveExecMu.Unlock()
}

func (r *Runner) unmarkExecutionLive(execID string) {
	r.liveExecMu.Lock()
	delete(r.liveExecs, execID)
	r.liveExecMu.Unlock()
}

// IsExecutionLive reports whether execID is currently being executed by
// this process (see liveExecs).
func (r *Runner) IsExecutionLive(execID string) bool {
	r.liveExecMu.Lock()
	_, ok := r.liveExecs[execID]
	r.liveExecMu.Unlock()
	return ok
}

// markActive/unmarkActive/IsWorkflowActive back IsWorkflowActive; see
// its doc comment and the activeRuns field comment above.
func (r *Runner) markActive(workflowID string) {
	r.activeMu.Lock()
	if r.activeRuns == nil {
		r.activeRuns = map[string]int{}
	}
	r.activeRuns[workflowID]++
	r.activeMu.Unlock()
}

func (r *Runner) unmarkActive(workflowID string) {
	r.activeMu.Lock()
	if r.activeRuns[workflowID] <= 1 {
		delete(r.activeRuns, workflowID)
	} else {
		r.activeRuns[workflowID]--
	}
	r.activeMu.Unlock()
}

// IsWorkflowActive reports whether workflowID currently has a run in
// flight through this Runner -- queued or running, via any trigger
// path (manual/async, scheduler, webhook). Used by the workflow-delete
// endpoint as a fast, always-available check that also covers
// scheduler/webhook-triggered synchronous runs, which (unlike
// Manager-started runs) write no Postgres row at all until they
// finish -- see RunFromNode/runOnce -- so a Postgres-only check would
// miss them entirely. The store's own transactional check
// (store.Store.DeleteWorkflow) remains the actual correctness
// guarantee for the async/Manager path; this is a fast-fail
// complement, not a replacement.
func (r *Runner) IsWorkflowActive(workflowID string) bool {
	r.activeMu.Lock()
	defer r.activeMu.Unlock()
	return r.activeRuns[workflowID] > 0
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
	// Service-level concurrency is hard-capped at 1: no setting (including
	// MICROFLOW_MAX_CONCURRENT_EXECUTIONS) may let two Services/workflows
	// run at the same time. maxConcurrent is accepted only for API
	// compatibility.
	_ = maxConcurrent
	r.sem = make(chan struct{}, 1)
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
// WithRecovery attaches the durable checkpoint store used by crash/restart recovery.
func (r *Runner) WithRecovery(s CheckpointStore) *Runner {
	r.Recovery = s
	return r
}

// WithAutoOptimize attaches the Auto Optimize layer (see internal/autoopt).
// Optional -- a Runner without it behaves exactly as before.
func (r *Runner) WithAutoOptimize(o *autoopt.Optimizer) *Runner {
	r.AutoOpt = o
	return r
}

func (r *Runner) WithNodeRunCap(n int) *Runner {
	r.NodeRunCap = n
	return r
}

// WithEnv attaches the Global/Service Environment resolver (see
// vault.EnvVault) used to populate every future RunFromNode call's
// RunContext.ServiceEnv/GlobalEnv. Optional -- a Runner with no
// resolver behaves exactly as before (rule 14).
func (r *Runner) WithEnv(e EnvResolver) *Runner {
	r.Env = e
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

	// Marked active for this whole call (queued-for-a-slot included),
	// not just while engine.Run is executing -- see IsWorkflowActive's
	// doc comment for why this matters for scheduler/webhook runs.
	r.markActive(wf.ID)
	defer r.unmarkActive(wf.ID)

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

	return r.runOnce(runCtx, wf, newID(), startNode, mode, seed, nil, nil)
}

// runOnce is the single place that actually builds a RunContext, calls
// engine.Run, persists the result, and cleans up scratch -- shared by
// the synchronous RunFromNode above and the async Manager (async.go).
// execID is supplied by the caller (Manager pre-allocates it so it can
// hand the id back to the HTTP client before the run starts).
// onNodeRun, if non-nil, is wired to RunContext.OnNodeRun for live
// per-node progress (SSE); RunFromNode's callers (scheduler/webhook)
// don't need it and pass nil.
func (r *Runner) runOnce(ctx context.Context, wf *model.Workflow, execID, startNode, mode string, seed model.NodeOutput, onNodeRun func(model.NodeRunResult), resume *model.ExecutionCheckpoint) (*model.Execution, error) {
	// Registered before the execution row/first checkpoint exist and released
	// only after the terminal cleanup below, so the recovery loop can never
	// treat this still-running execution as an orphan (see liveExecs).
	r.markExecutionLive(execID)
	defer r.unmarkExecutionLive(execID)

	// Auto Optimize: this execution's ownership record. The deferred Release is
	// the reliable "finally" -- it runs on success, error, timeout, cancel and
	// panic alike, and only ever touches resources registered by THIS execution.
	// Both calls are no-ops when Auto Optimize is off (nil receiver).
	owned := r.AutoOpt.Begin(execID, wf.ID)
	defer owned.Release()

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

	startAt := time.Now()
	execution := &model.Execution{ID: execID, WorkflowID: wf.ID, ServiceID: wf.ServiceID, Mode: mode, Status: model.StatusRunning, StartedAt: startAt}
	if resume != nil {
		if !resume.StartedAt.IsZero() {
			execution.StartedAt = resume.StartedAt
		}
		execution.Status = resume.State.Status
		if execution.Status == "" {
			execution.Status = model.StatusRunning
		}
	}
	rc := &engine.RunContext{
		Workflow:    wf,
		Execution:   execution,
		StaticData:  r.StaticData,
		Credentials: r.Credentials,
		ScratchDir:  scratchDir,
		Owned:       owned,
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
	// One decrypt pass per run, not per $env lookup (rule: don't hit the
	// database/AEAD once per Code-node call in a hot per-item loop -- see
	// vault.EnvVault.ResolveAll's doc comment). A resolve failure is
	// logged and left as nil rather than failing the whole run: falling
	// through to the process environment is the documented, safe
	// pre-Service-isolation behavior (rule 14), not a silent secret leak
	// -- it only ever narrows what a Code node/Sheets node can see.
	if r.Env != nil {
		serviceEnv, globalEnv, err := r.Env.ResolveAll(ctx, wf.ServiceID)
		if err != nil {
			log.Printf("runner: environment resolve failed for service %q (execution %s): %v -- falling back to process environment", wf.ServiceID, execID, err)
		} else {
			rc.ServiceEnv = serviceEnv
			rc.GlobalEnv = globalEnv
			for k, v := range globalEnv {
				rc.Redactor.NoteEnvValue(k, v)
			}
			for k, v := range serviceEnv {
				rc.Redactor.NoteEnvValue(k, v)
			}
		}
	}

	// Persist the execution row before the first checkpoint. The checkpoint
	// table has a foreign key to executions, and this also makes synchronous
	// scheduler/webhook runs visible/recoverable before their first node.
	if resume == nil && r.Recovery != nil {
		if err := r.Execs.SaveExecution(context.Background(), execution); err != nil {
			return nil, fmt.Errorf("runner: persist initial execution %q: %w", execID, err)
		}
	} else if resume != nil {
		if err := r.Execs.SaveExecution(context.Background(), execution); err != nil {
			return nil, fmt.Errorf("runner: persist recovered execution %q: %w", execID, err)
		}
	}

	var resumeState *model.ExecutionCheckpointState
	if resume != nil {
		resumeState = &resume.State
	}
	if r.Recovery != nil {
		hash, hashErr := workflowHash(wf)
		if hashErr != nil {
			return nil, fmt.Errorf("runner: workflow hash: %w", hashErr)
		}
		checkpointFn := func(state model.ExecutionCheckpointState) error {
			cp := &model.ExecutionCheckpoint{
				ExecutionID: execID, WorkflowID: wf.ID, WorkflowHash: hash, WorkflowUpdatedAt: wf.UpdatedAt,
				Mode: mode, StartedAt: execution.StartedAt, State: state, UpdatedAt: time.Now(),
			}
			if resume != nil {
				cp.LeaseOwner = resume.LeaseOwner
				cp.LeaseUntil = time.Now().Add(recoveryLease())
			}
			// Slow/cold remote Postgres (e.g. Neon) can exceed a short
			// deadline; retry transient failures with a fresh timeout.
			var err error
			for attempt := 1; attempt <= 3; attempt++ {
				checkCtx, cancel := context.WithTimeout(context.Background(), checkpointTimeout())
				err = r.Recovery.SaveExecutionCheckpoint(checkCtx, cp)
				cancel()
				if err == nil || errors.Is(err, model.ErrCheckpointTooLarge) {
					break
				}
				log.Printf("checkpoint save attempt %d/3 failed execution=%s: %v", attempt, execID, err)
				time.Sleep(time.Duration(attempt) * time.Second)
			}
			if err != nil {
				return err
			}
			log.Printf("checkpoint saved execution=%s status=%s steps=%d", execID, state.Status, state.Steps)
			return nil
		}
		rc.Checkpoint = checkpointFn
	}
	ex, runErr := r.Engine.RunWithCheckpoint(ctx, rc, startNode, seed, resumeState)

	// Always persist, even on failure -- the execution panel/history
	// needs the error/status either way (rule: execution panel shows
	// status/input/output/error/logs/duration regardless of outcome).
	if saveErr := r.Execs.SaveExecution(context.Background(), ex); saveErr != nil {
		log.Printf("runner: failed to persist execution %s: %v", execID, saveErr)
	}
	if r.Recovery != nil && ex != nil {
		if ex.Status == model.StatusSuccess || ex.Status == model.StatusError || ex.Status == model.StatusCancelled {
			if err := r.Recovery.DeleteExecutionCheckpoint(context.Background(), execID); err != nil {
				log.Printf("checkpoint delete execution=%s failed: %v", execID, err)
			} else {
				log.Printf("checkpoint deleted execution=%s", execID)
			}
		}
	}
	// Execution history is intentionally ephemeral on this low-storage deployment.
	// Keep the in-memory result long enough for the live response/event stream, but
	// remove the durable terminal row immediately so PostgreSQL cannot grow from
	// completed executions. The checkpoint FK is ON DELETE CASCADE as a final
	// safety net.
	if ex != nil && (ex.Status == model.StatusSuccess || ex.Status == model.StatusError || ex.Status == model.StatusCancelled) {
		if d, ok := r.Execs.(ExecutionDeleter); ok {
			if err := d.DeleteExecution(context.Background(), execID); err != nil {
				log.Printf("execution cleanup execution=%s failed: %v", execID, err)
			} else {
				log.Printf("execution cleanup deleted execution=%s", execID)
			}
		}
	}
	r.cleanupScratch(scratchDir)

	return ex, runErr
}

// SaveInitialCheckpoint records the minimal starting state before an async
// execution is handed to the worker pool. This closes the crash window between
// POST /execute and the first node execution.
func (r *Runner) SaveInitialCheckpoint(ctx context.Context, wf *model.Workflow, ex *model.Execution, startNode string, seed model.NodeOutput) error {
	if r.Recovery == nil {
		return nil
	}
	hash, err := workflowHash(wf)
	if err != nil {
		return err
	}
	return r.Recovery.SaveExecutionCheckpoint(ctx, &model.ExecutionCheckpoint{
		ExecutionID: ex.ID, WorkflowID: wf.ID, WorkflowHash: hash, WorkflowUpdatedAt: wf.UpdatedAt,
		Mode: ex.Mode, StartedAt: ex.StartedAt,
		State: model.ExecutionCheckpointState{Version: checkpointVersion, Pending: []model.CheckpointQueueItem{{NodeName: startNode, Input: seed}}, Status: ex.Status},
	})
}

// FirstTriggerNode picks a start node when the caller doesn't specify
// one (manual "Execute" button with nothing selected). Enabled triggers
// win over disabled ones (a disabled Manual Trigger must not be chosen
// over an active Schedule Trigger -- the engine skips a disabled start
// node, so the run would do nothing), and names are visited in sorted
// order so the choice is deterministic instead of depending on Go's
// random map iteration. If every trigger is disabled, one is still
// returned, as before.
func FirstTriggerNode(wf *model.Workflow) string {
	names := make([]string, 0, len(wf.Nodes))
	for name := range wf.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	fallback := ""
	for _, name := range names {
		n := wf.Nodes[name]
		if n == nil {
			continue
		}
		switch n.Type {
		case model.TypeManualTrigger, model.TypeScheduleTrigger, model.TypeWebhookTrigger:
			if !n.Disabled {
				return name
			}
			if fallback == "" {
				fallback = name
			}
		}
	}
	return fallback
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (r *Runner) cleanupScratch(dir string) {
	r.scratchWG.Add(1)
	go func() {
		defer r.scratchWG.Done()
		_ = os.RemoveAll(dir)
	}()
}

// WaitScratchCleanup blocks until every scratch directory removal started by a
// finished run has completed, or ctx ends. Only directories of executions that
// already finished are ever removed (never a live or checkpoint-recoverable
// one), so this is the safe part of "clean temporary resources between runs".
func (r *Runner) WaitScratchCleanup(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		r.scratchWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// checkpointTimeout bounds one checkpoint write attempt. Override with
// MICROFLOW_CHECKPOINT_TIMEOUT_SECONDS (default 45s).
func checkpointTimeout() time.Duration {
	if v := os.Getenv("MICROFLOW_CHECKPOINT_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 5 && n <= 600 {
			return time.Duration(n) * time.Second
		}
	}
	return 45 * time.Second
}
