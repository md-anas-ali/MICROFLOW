// UNVERIFIED IN SANDBOX: written but never run here. See STATUS.md.
package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"microflow/internal/model"
)

// fakeStaticData is an in-memory StaticDataStore for tests, standing in
// for the Postgres row-lock implementation.
type fakeStaticData struct {
	mu   sync.Mutex
	data map[string]map[string]any
}

func newFakeStaticData() *fakeStaticData {
	return &fakeStaticData{data: map[string]map[string]any{}}
}

func (f *fakeStaticData) WithLock(_ context.Context, workflowID string, fn func(map[string]any) (map[string]any, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.data[workflowID]
	if d == nil {
		d = map[string]any{}
	}
	newD, err := fn(d)
	if err != nil {
		return err
	}
	f.data[workflowID] = newD
	return nil
}

// echoExecutor just returns its input unchanged on output 0.
type echoExecutor struct{ calls int }

func (e *echoExecutor) Execute(_ context.Context, _ *RunContext, _ *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	e.calls++
	if len(input) == 0 {
		return model.NodeOutput{{{JSON: map[string]any{}}}}, nil
	}
	return input, nil
}

// failNTimesExecutor fails its first N-1 calls, then succeeds -- used to
// test the per-node retry loop.
type failNTimesExecutor struct {
	failUntil int
	calls     int
}

func (e *failNTimesExecutor) Execute(_ context.Context, _ *RunContext, _ *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	e.calls++
	if e.calls < e.failUntil {
		return nil, errors.New("simulated transient failure")
	}
	return input, nil
}

func buildTestRunContext(wf *model.Workflow) *RunContext {
	return &RunContext{
		Workflow:   wf,
		Execution:  &model.Execution{ID: "test-exec", WorkflowID: wf.ID, Mode: "manual"},
		StaticData: newFakeStaticData(),
	}
}

func TestEngineLinearRun(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf1",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "echo"},
			"B": {Name: "B", Type: "echo"},
		},
		Connections: map[string][]model.Connection{
			"A": {{SourceName: "A", SourceIndex: 0, TargetName: "B", TargetIndex: 0}},
		},
	}
	e := New(map[model.NodeType]NodeExecutor{"echo": &echoExecutor{}})
	rc := buildTestRunContext(wf)
	seed := model.NodeOutput{{{JSON: map[string]any{"x": 1.0}}}}

	ex, err := e.Run(context.Background(), rc, "A", seed)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ex.Status != model.StatusSuccess {
		t.Errorf("status = %s, want success", ex.Status)
	}
	if len(ex.NodeRuns) != 2 {
		t.Errorf("got %d node runs, want 2", len(ex.NodeRuns))
	}
}

func TestEngineRetriesFailingNode(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf2",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "flaky", RetryOnFail: true, MaxTries: 3, WaitBetweenTriesMs: 1},
		},
	}
	flaky := &failNTimesExecutor{failUntil: 3}
	e := New(map[model.NodeType]NodeExecutor{"flaky": flaky})
	rc := buildTestRunContext(wf)

	ex, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ex.Status != model.StatusSuccess {
		t.Fatalf("status = %s, want success after retries", ex.Status)
	}
	if flaky.calls != 3 {
		t.Errorf("flaky.calls = %d, want 3", flaky.calls)
	}
}

func TestEngineUnknownNodeTypeFailsLoudly(t *testing.T) {
	wf := &model.Workflow{
		ID:    "wf3",
		Nodes: map[string]*model.Node{"A": {Name: "A", Type: model.TypeUnknown}},
	}
	e := New(map[model.NodeType]NodeExecutor{}) // nothing registered
	rc := buildTestRunContext(wf)

	_, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err == nil {
		t.Fatal("expected error for unsupported node type, got nil (rule: never silently skip required functionality)")
	}
}

func TestEngineMaxStepsGuardsInfiniteLoop(t *testing.T) {
	// A -> A (self loop), which would spin forever without MaxSteps.
	wf := &model.Workflow{
		ID:    "wf4",
		Nodes: map[string]*model.Node{"A": {Name: "A", Type: "echo"}},
		Connections: map[string][]model.Connection{
			"A": {{SourceName: "A", SourceIndex: 0, TargetName: "A", TargetIndex: 0}},
		},
	}
	e := New(map[model.NodeType]NodeExecutor{"echo": &echoExecutor{}})
	e.MaxSteps = 50
	rc := buildTestRunContext(wf)

	_, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err == nil {
		t.Fatal("expected MaxSteps error for infinite self-loop")
	}
}
