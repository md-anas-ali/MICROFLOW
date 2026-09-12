package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

// blockingExecutor is a NodeExecutor that blocks until release is
// closed, so tests can deterministically observe "a run is currently
// in flight" before letting it finish.
type blockingExecutor struct {
	entered chan struct{}
	release chan struct{}
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	select {
	case <-b.entered:
	default:
		close(b.entered)
	}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return model.NodeOutput{{{JSON: map[string]any{}}}}, nil
}

// memWorkflows/memExecs are trivial in-memory stand-ins for
// runner.WorkflowLoader/ExecutionSaver -- no Postgres needed for these
// tests, which only exercise Runner/Manager's own bookkeeping.
type memWorkflows struct {
	mu  sync.Mutex
	wfs map[string]*model.Workflow
}

func (m *memWorkflows) LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wf, ok := m.wfs[id]
	if !ok {
		return nil, errNotFound
	}
	return wf, nil
}

type jsonErr2 string

func (e jsonErr2) Error() string { return string(e) }

var errNotFound = jsonErr2("workflow not found")

type memExecs struct{}

func (memExecs) SaveExecution(ctx context.Context, ex *model.Execution) error { return nil }

func testWorkflow(id string) *model.Workflow {
	return &model.Workflow{
		ID:   id,
		Name: "test",
		Nodes: map[string]*model.Node{
			"Start": {ID: "1", Name: "Start", Type: model.TypeManualTrigger},
		},
	}
}

func newTestRunner(wfID string, exec *blockingExecutor) (*Runner, *memWorkflows) {
	registry := map[model.NodeType]engine.NodeExecutor{
		model.TypeManualTrigger: exec,
	}
	eng := engine.New(registry)
	wfs := &memWorkflows{wfs: map[string]*model.Workflow{wfID: testWorkflow(wfID)}}
	r := New(wfs, memExecs{}, eng, nil, nil, "/tmp")
	r.Timeout = 5 * time.Second
	return r, wfs
}

// TestIsWorkflowActive_RunFromNode confirms a synchronous
// scheduler/webhook-style run (RunFromNode) is reported active for its
// entire duration and inactive immediately after, even though it
// writes no Postgres row until it finishes (see RunFromNode/runOnce).
func TestIsWorkflowActive_RunFromNode(t *testing.T) {
	const wfID = "wf-sync"
	exec := newBlockingExecutor()
	r, _ := newTestRunner(wfID, exec)

	if r.IsWorkflowActive(wfID) {
		t.Fatal("workflow reported active before any run started")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = r.RunFromNode(context.Background(), wfID, "Start", "manual", model.NodeOutput{{{JSON: map[string]any{}}}})
	}()

	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("run never reached the blocking node")
	}

	if !r.IsWorkflowActive(wfID) {
		t.Fatal("workflow should be reported active while a run is in flight")
	}

	close(exec.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run never finished after release")
	}

	if r.IsWorkflowActive(wfID) {
		t.Fatal("workflow should not be reported active once the run has finished")
	}
}

// TestIsWorkflowActive_Manager confirms the async Manager path marks a
// workflow active from Start (still queued) through the run's actual
// completion, matching the delete endpoint's expectations.
func TestIsWorkflowActive_Manager(t *testing.T) {
	const wfID = "wf-async"
	exec := newBlockingExecutor()
	r, _ := newTestRunner(wfID, exec)
	mgr := NewManager(r, 1, 2)

	if r.IsWorkflowActive(wfID) {
		t.Fatal("workflow reported active before Start was called")
	}

	execID, err := mgr.Start(context.Background(), wfID, "Start", "manual", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("run never reached the blocking node")
	}

	if !r.IsWorkflowActive(wfID) {
		t.Fatal("workflow should be reported active while queued/running through the Manager")
	}

	close(exec.release)

	deadline := time.After(2 * time.Second)
	for r.IsWorkflowActive(wfID) {
		select {
		case <-deadline:
			t.Fatal("workflow still reported active long after the run finished")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if ex, ok := mgr.Get(execID); !ok || ex.Status != model.StatusSuccess {
		t.Fatalf("expected finished successful execution, got %+v (ok=%v)", ex, ok)
	}
}

// TestIsWorkflowActive_DistinctWorkflows confirms the refcount is keyed
// per workflow ID -- an active run on one workflow must never make an
// unrelated workflow look active (which would wrongly block its
// delete).
func TestIsWorkflowActive_DistinctWorkflows(t *testing.T) {
	const wfA, wfB = "wf-a", "wf-b"
	exec := newBlockingExecutor()
	r, wfs := newTestRunner(wfA, exec)
	wfs.mu.Lock()
	wfs.wfs[wfB] = testWorkflow(wfB)
	wfs.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = r.RunFromNode(context.Background(), wfA, "Start", "manual", model.NodeOutput{{{JSON: map[string]any{}}}})
	}()

	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("run never reached the blocking node")
	}

	if r.IsWorkflowActive(wfB) {
		t.Fatal("unrelated workflow B must not be reported active while A is running")
	}
	if !r.IsWorkflowActive(wfA) {
		t.Fatal("workflow A should be reported active")
	}

	close(exec.release)
	<-done
}
