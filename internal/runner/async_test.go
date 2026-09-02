package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

// --- shared fakes -----------------------------------------------------

type fakeWorkflows struct {
	mu  sync.Mutex
	wfs map[string]*model.Workflow
}

func (f *fakeWorkflows) LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wf, ok := f.wfs[id]
	if !ok {
		return nil, errNotFound
	}
	return wf, nil
}

type errString string

func (e errString) Error() string { return string(e) }

const errNotFound = errString("workflow not found")

type fakeExecs struct {
	mu    sync.Mutex
	saved []*model.Execution
}

func (f *fakeExecs) SaveExecution(ctx context.Context, ex *model.Execution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Store a defensive copy -- the caller (runOnce/Manager) may keep
	// mutating its own *model.Execution after this returns, and tests
	// read f.saved concurrently with that.
	cp := *ex
	cp.NodeRuns = append([]model.NodeRunResult(nil), ex.NodeRuns...)
	f.saved = append(f.saved, &cp)
	return nil
}

func (f *fakeExecs) last() *model.Execution {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saved) == 0 {
		return nil
	}
	return f.saved[len(f.saved)-1]
}

// instantExecutor completes immediately with one output item -- used
// as the sole node in a one-node workflow so a run finishes fast.
type instantExecutor struct{}

func (instantExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	return model.NodeOutput{{{JSON: map[string]any{"ok": true}}}}, nil
}

// blockingExecutor doesn't return until its context is cancelled or a
// release channel is closed -- used to hold a worker slot open so
// tests can observe queued/running state and exercise cancellation.
type blockingExecutor struct {
	release <-chan struct{}
}

func (b blockingExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	select {
	case <-b.release:
		return model.NodeOutput{{{JSON: map[string]any{"ok": true}}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func oneNodeWorkflow(id string) *model.Workflow {
	return &model.Workflow{
		ID:          id,
		Name:        "test",
		Nodes:       map[string]*model.Node{"Start": {Name: "Start", Type: model.TypeManualTrigger}},
		Connections: map[string][]model.Connection{},
	}
}

func newTestRunner(t *testing.T, registry map[model.NodeType]engine.NodeExecutor, wf *model.Workflow) (*Runner, *fakeExecs) {
	t.Helper()
	wfs := &fakeWorkflows{wfs: map[string]*model.Workflow{wf.ID: wf}}
	execs := &fakeExecs{}
	eng := engine.New(registry)
	r := New(wfs, execs, eng, fakeStaticData{}, fakeCreds{}, t.TempDir())
	r.Timeout = 5 * time.Second
	return r, execs
}

type fakeStaticData struct{}

func (fakeStaticData) WithLock(ctx context.Context, workflowID string, fn func(map[string]any) (map[string]any, error)) error {
	_, err := fn(map[string]any{})
	return err
}

type fakeCreds struct{}

func (fakeCreds) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	return nil, nil
}

// --- tests --------------------------------------------------------------

// TestManagerStartReturnsBeforeCompletion is the core async contract
// (spec section M): Start must not block until the run finishes.
func TestManagerStartReturnsBeforeCompletion(t *testing.T) {
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, wf)
	m := NewManager(r, 2, 5)

	start := time.Now()
	execID, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Start took %v -- should return immediately, not wait for the run", elapsed)
	}
	if execID == "" {
		t.Fatal("Start returned empty execution id")
	}

	// The execution should reach a terminal state shortly after, without
	// the caller having done anything else.
	deadline := time.Now().Add(2 * time.Second)
	var final *model.Execution
	for time.Now().Before(deadline) {
		ex, ok := m.Get(execID)
		if ok && (ex.Status == model.StatusSuccess || ex.Status == model.StatusError) {
			final = ex
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final == nil {
		t.Fatal("execution never reached a terminal state")
	}
	if final.Status != model.StatusSuccess {
		t.Fatalf("status = %q, want success (error=%q)", final.Status, final.Error)
	}
	if len(final.NodeRuns) != 1 {
		t.Fatalf("NodeRuns = %d, want 1", len(final.NodeRuns))
	}
}

// TestManagerGetTransitionsThroughQueuedAndRunning uses a blocking
// executor to observe StatusQueued/StatusRunning before release.
func TestManagerGetTransitionsThroughQueuedAndRunning(t *testing.T) {
	release := make(chan struct{})
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: blockingExecutor{release: release}}, wf)
	m := NewManager(r, 1, 5)

	execID, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should observe running (queued is very transient with a free
	// worker slot, so we don't assert on it directly here -- see the
	// queue-full test below for that).
	deadline := time.Now().Add(1 * time.Second)
	sawRunning := false
	for time.Now().Before(deadline) {
		ex, ok := m.Get(execID)
		if ok && ex.Status == model.StatusRunning {
			sawRunning = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawRunning {
		t.Fatal("never observed StatusRunning before release")
	}

	close(release)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ex, ok := m.Get(execID)
		if ok && ex.Status == model.StatusSuccess {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("execution never completed after release")
}

// TestManagerQueueFullBackpressure verifies Start rejects work once
// maxQueued accepted-but-unfinished executions are outstanding (rule
// 19: reject excess work rather than queue unboundedly).
func TestManagerQueueFullBackpressure(t *testing.T) {
	release := make(chan struct{}) // never closed during this test
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: blockingExecutor{release: release}}, wf)
	m := NewManager(r, 1, 2) // 1 concurrent worker, queue cap 2

	id1, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start 1: %v", err)
	}
	id2, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start 2: %v", err)
	}
	_, err = m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != ErrQueueFull {
		t.Fatalf("Start 3: got err=%v, want ErrQueueFull", err)
	}

	// Unblock the one running job so the test can clean up promptly;
	// the second queued job would then start and also block forever on
	// `release`, so cancel it instead of waiting on it.
	close(release)
	_ = m.Cancel(id1)
	_ = m.Cancel(id2)
}

// TestManagerCancelWhileQueued verifies a job waiting for a worker
// slot (never actually started) is cancelled cleanly rather than
// eventually running anyway.
func TestManagerCancelWhileQueued(t *testing.T) {
	release := make(chan struct{}) // first job never released during this test
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: blockingExecutor{release: release}}, wf)
	m := NewManager(r, 1, 2)

	holder, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start holder: %v", err)
	}
	queued, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start queued: %v", err)
	}

	if err := m.Cancel(queued); err != nil {
		t.Fatalf("Cancel queued job: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ex, ok := m.Get(queued)
		if ok && ex.Status == model.StatusCancelled {
			close(release)
			_ = m.Cancel(holder)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	t.Fatal("queued job never reached StatusCancelled")
}

// TestManagerSubscribeReceivesTerminalEvent verifies the SSE-facing
// Subscribe API delivers events ending in a terminal type and then
// closes the channel.
func TestManagerSubscribeReceivesTerminalEvent(t *testing.T) {
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, wf)
	m := NewManager(r, 2, 5)

	execID, err := m.Start(context.Background(), "wf1", "", "manual", nil)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	events, _, unsubscribe, ok := m.Subscribe(execID)
	if !ok {
		t.Fatal("Subscribe: execution not found")
	}
	defer unsubscribe()

	var gotTerminal bool
	deadline := time.After(2 * time.Second)
	for !gotTerminal {
		select {
		case ev, chOK := <-events:
			if !chOK {
				gotTerminal = true // channel closed = terminal
				break
			}
			if ev.Type == EventExecutionCompleted || ev.Type == EventExecutionFailed || ev.Type == EventExecutionCancelled {
				gotTerminal = true
			}
		case <-deadline:
			t.Fatal("never received a terminal event")
		}
	}
}

// TestManagerSubscribeUnknownExecution verifies Subscribe reports
// ok=false for an id the Manager has no record of, rather than
// blocking or panicking.
func TestManagerSubscribeAllReceivesExecutionEvents(t *testing.T) {
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, wf)
	m := NewManager(r, 1, 2)
	events, _, unsubscribe := m.SubscribeAll()
	defer unsubscribe()

	if _, err := m.Start(context.Background(), "wf1", "", "manual", nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == EventExecutionStarted || ev.Type == EventExecutionCompleted {
				return
			}
		case <-deadline:
			t.Fatal("global execution event stream produced no event")
		}
	}
}

func TestManagerSubscribeUnknownExecution(t *testing.T) {
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{}, wf)
	m := NewManager(r, 1, 1)

	_, _, _, ok := m.Subscribe("does-not-exist")
	if ok {
		t.Fatal("Subscribe: expected ok=false for unknown execution id")
	}
}

// TestManagerStartUnsupportedNode verifies Start still surfaces a
// clear synchronous error for a bad request (missing executor) rather
// than accepting it and failing invisibly in the background -- callers
// (the API layer) map this to an immediate 4xx.
func TestManagerStartMissingWorkflow(t *testing.T) {
	wf := oneNodeWorkflow("wf1")
	r, _ := newTestRunner(t, map[model.NodeType]engine.NodeExecutor{}, wf)
	m := NewManager(r, 1, 1)

	_, err := m.Start(context.Background(), "does-not-exist", "", "manual", nil)
	if err == nil {
		t.Fatal("Start: expected an error for an unknown workflow id")
	}
}
