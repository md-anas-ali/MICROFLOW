package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"microflow/internal/model"
)

type recoveryExecutor struct {
	mu    sync.Mutex
	calls map[string]int
	fn    func(*model.Node) (model.NodeOutput, error)
}

func (e *recoveryExecutor) Execute(_ context.Context, _ *RunContext, n *model.Node, in model.NodeOutput) (model.NodeOutput, error) {
	e.mu.Lock()
	e.calls[n.Name]++
	e.mu.Unlock()
	if e.fn != nil {
		return e.fn(n)
	}
	return in, nil
}

func testWorkflow(nodes ...*model.Node) *model.Workflow {
	m := map[string]*model.Node{}
	for _, n := range nodes {
		m[n.Name] = n
	}
	return &model.Workflow{ID: "wf", Nodes: m, Connections: map[string][]model.Connection{}}
}

func TestRunWithCheckpointResumesFromQueue(t *testing.T) {
	a := &model.Node{Name: "A", Type: model.TypeNoOp}
	b := &model.Node{Name: "B", Type: model.TypeNoOp}
	wf := testWorkflow(a, b)
	wf.Connections["A"] = []model.Connection{{SourceName: "A", TargetName: "B"}}

	exec := &recoveryExecutor{calls: map[string]int{}}
	eng := New(map[model.NodeType]NodeExecutor{model.TypeNoOp: exec})
	first := true
	var saved model.ExecutionCheckpointState
	rc := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "ex1"}, Checkpoint: func(s model.ExecutionCheckpointState) error {
		if s.Status == model.StatusRunning && s.LastCompleted == "A" && first {
			saved = s
			first = false
			return errors.New("simulated crash after durable checkpoint")
		}
		return nil
	}}
	_, _ = eng.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{"x": 1}}}})
	if exec.calls["A"] != 1 || exec.calls["B"] != 0 {
		t.Fatalf("first run calls A=%d B=%d", exec.calls["A"], exec.calls["B"])
	}
	if len(saved.Pending) != 1 || saved.Pending[0].NodeName != "B" {
		t.Fatalf("checkpoint did not retain downstream queue: %#v", saved.Pending)
	}

	rc2 := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "ex1"}, Checkpoint: func(model.ExecutionCheckpointState) error { return nil }}
	_, err := eng.RunWithCheckpoint(context.Background(), rc2, "A", nil, &saved)
	if err != nil {
		t.Fatal(err)
	}
	if exec.calls["A"] != 1 || exec.calls["B"] != 1 {
		t.Fatalf("resume reran completed node: A=%d B=%d", exec.calls["A"], exec.calls["B"])
	}
}

func TestRunWithCheckpointPreservesAttemptsAndOutputs(t *testing.T) {
	a := &model.Node{Name: "A", Type: model.TypeNoOp}
	wf := testWorkflow(a)
	exec := &recoveryExecutor{calls: map[string]int{}}
	eng := New(map[model.NodeType]NodeExecutor{model.TypeNoOp: exec})
	resume := &model.ExecutionCheckpointState{
		Version: 1, Pending: []model.CheckpointQueueItem{{NodeName: "A", Input: model.NodeOutput{{{JSON: map[string]any{"from": "checkpoint"}}}}}},
		NodeAttempts: map[string]int{"Old": 3}, NodeOutputs: map[string]map[string]any{"Old": {"value": 42}}, Steps: 7, Status: model.StatusRunning,
	}
	rc := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "ex2"}}
	_, err := eng.RunWithCheckpoint(context.Background(), rc, "A", nil, resume)
	if err != nil {
		t.Fatal(err)
	}
	if exec.calls["A"] != 1 {
		t.Fatal("expected pending node to execute once")
	}
	ctx := rc.ExprContext(map[string]any{})
	if got := ctx.NodeOutputs["Old"]["value"]; got != 42 {
		t.Fatalf("node output context lost: %#v", ctx.NodeOutputs)
	}
}

func TestWaitCheckpointCapturesDeadline(t *testing.T) {
	w := &model.Node{Name: "Wait", Type: model.TypeWait, Parameters: map[string]any{"amount": float64(0.15), "unit": "seconds"}}
	wf := testWorkflow(w)
	eng := New(map[model.NodeType]NodeExecutor{model.TypeWait: waitTestExecutor{}})
	var saved model.ExecutionCheckpointState
	rc := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "ex3"}, Checkpoint: func(s model.ExecutionCheckpointState) error {
		if s.Status == model.StatusWaiting {
			saved = s
		}
		return nil
	}}
	started := time.Now()
	_, err := eng.Run(context.Background(), rc, "Wait", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != model.StatusWaiting || saved.Wait == nil || len(saved.Pending) != 1 || saved.Pending[0].NodeName != "Wait" {
		t.Fatalf("wait checkpoint incomplete: %#v", saved)
	}
	if saved.Wait.Until.Before(started) {
		t.Fatal("wait deadline was not captured")
	}
}

// waitTestExecutor keeps the engine test independent from internal/nodes.
type waitTestExecutor struct{}

func (waitTestExecutor) Execute(ctx context.Context, rc *RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	until := rc.ResumeWaitUntil
	if until.IsZero() {
		until = time.Now().Add(100 * time.Millisecond)
		if rc.BeforeWait != nil {
			if err := rc.BeforeWait(until); err != nil {
				return nil, err
			}
		}
	}
	t := time.NewTimer(time.Until(until))
	defer t.Stop()
	select {
	case <-t.C:
		return input, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestDeterministicOperationIDIsStable(t *testing.T) {
	got1 := deterministicOperationID("execution-1", 12, "HTTP Request")
	got2 := deterministicOperationID("execution-1", 12, "HTTP Request")
	got3 := deterministicOperationID("execution-1", 13, "HTTP Request")
	if got1 == "" || got1 != got2 || got1 == got3 {
		t.Fatalf("operation id stability/uniqueness broken: %q %q %q", got1, got2, got3)
	}
}

func TestRunWithCheckpointPersistsAttemptAndOperationBeforeNode(t *testing.T) {
	node := &model.Node{Name: "HTTP", Type: model.TypeNoOp}
	wf := testWorkflow(node)
	exec := &recoveryExecutor{calls: map[string]int{}}
	eng := New(map[model.NodeType]NodeExecutor{model.TypeNoOp: exec})
	var first model.ExecutionCheckpointState
	rc := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "ex-start"}, Checkpoint: func(s model.ExecutionCheckpointState) error {
		if s.Status == model.StatusRunning && s.LastCompleted == "" && len(s.Pending) == 1 && first.Status == "" {
			first = s
		}
		return nil
	}}
	_, err := eng.Run(context.Background(), rc, "HTTP", model.NodeOutput{{{JSON: map[string]any{"x": 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Pending) != 1 || first.Pending[0].NodeName != "HTTP" || first.Pending[0].OperationID == "" {
		t.Fatalf("missing node-start checkpoint operation id: %#v", first)
	}
	if first.NodeAttempts["HTTP"] != 1 {
		t.Fatalf("attempt count not persisted: %#v", first.NodeAttempts)
	}
}

func TestResumeUsesPersistedOperationID(t *testing.T) {
	node := &model.Node{Name: "SideEffect", Type: model.TypeNoOp}
	wf := testWorkflow(node)
	var got string
	exec := operationIDExecutor{fn: func(rc *RunContext) { got = rc.CurrentOperationID }}
	eng := New(map[model.NodeType]NodeExecutor{model.TypeNoOp: &exec})
	want := "stable-operation-123"
	resume := &model.ExecutionCheckpointState{
		Version: 1, Status: model.StatusRunning, Steps: 4,
		Pending: []model.CheckpointQueueItem{{NodeName: node.Name, OperationID: want, Input: model.NodeOutput{{{JSON: map[string]any{}}}}}},
	}
	rc := &RunContext{Workflow: wf, Execution: &model.Execution{ID: "resume-op"}}
	if _, err := eng.RunWithCheckpoint(context.Background(), rc, node.Name, nil, resume); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("operation id changed across recovery: got %q want %q", got, want)
	}
}

type operationIDExecutor struct{ fn func(*RunContext) }

func (e *operationIDExecutor) Execute(_ context.Context, rc *RunContext, _ *model.Node, in model.NodeOutput) (model.NodeOutput, error) {
	e.fn(rc)
	return in, nil
}
