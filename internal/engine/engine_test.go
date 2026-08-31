// UNVERIFIED IN SANDBOX: written but never run here. See STATUS.md.
package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

// alwaysFailExecutor always fails, optionally with a Permanent-wrapped
// error, so tests can assert the retry loop's behavior around
// transient vs permanent failures without a real network call.
type alwaysFailExecutor struct {
	calls     int
	permanent bool
	errMsg    string
}

func (e *alwaysFailExecutor) Execute(_ context.Context, _ *RunContext, _ *model.Node, _ model.NodeOutput) (model.NodeOutput, error) {
	e.calls++
	msg := e.errMsg
	if msg == "" {
		msg = "simulated failure"
	}
	err := errors.New(msg)
	if e.permanent {
		return nil, Permanent(err)
	}
	return nil, err
}

// TestEnginePermanentErrorSkipsRemainingRetries verifies that once a
// node executor marks an error Permanent (e.g. an HTTP 401/403
// configuration error), the engine stops retrying immediately instead
// of burning the rest of the node's configured MaxTries budget on
// attempts guaranteed to fail identically.
func TestEnginePermanentErrorSkipsRemainingRetries(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf-permanent",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "alwaysFail", RetryOnFail: true, MaxTries: 5, WaitBetweenTriesMs: 1},
		},
	}
	exec := &alwaysFailExecutor{permanent: true, errMsg: "credential/configuration error"}
	e := New(map[model.NodeType]NodeExecutor{"alwaysFail": exec})
	rc := buildTestRunContext(wf)

	ex, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err == nil {
		t.Fatal("expected run error for a permanently failing required node")
	}
	if exec.calls != 1 {
		t.Errorf("alwaysFail.calls = %d, want 1 (permanent error must not consume the retry budget)", exec.calls)
	}
	if ex.Status != model.StatusError {
		t.Errorf("status = %s, want error", ex.Status)
	}
}

// TestEngineTransientErrorUsesFullRetryBudget is the mirror check: a
// non-permanent error DOES consume the configured MaxTries budget
// (existing behavior, unchanged by the permanent-error short-circuit).
func TestEngineTransientErrorUsesFullRetryBudget(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf-transient",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "alwaysFail", RetryOnFail: true, MaxTries: 3, WaitBetweenTriesMs: 1},
		},
	}
	exec := &alwaysFailExecutor{permanent: false}
	e := New(map[model.NodeType]NodeExecutor{"alwaysFail": exec})
	rc := buildTestRunContext(wf)

	_, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err == nil {
		t.Fatal("expected run error after exhausting retries")
	}
	if exec.calls != 3 {
		t.Errorf("alwaysFail.calls = %d, want 3 (full MaxTries budget for a transient error)", exec.calls)
	}
}

// TestEngineContinueOnFailPropagatesErrorItem verifies the fix to a
// real silent-failure bug: a node with onError=continueRegularOutput
// (the legacy bare continueOnFail:true behaves the same way) that
// fails must still send a pass-through item -- carrying the error --
// to its normal downstream connections, not just swallow the failure
// and stop. Previously nothing flowed downstream at all, which
// silently truncated the workflow while still reporting overall
// success.
func TestEngineContinueOnFailPropagatesErrorItem(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf-continue",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "alwaysFail", ContinueOnFail: true, OnErrorMode: "continueRegularOutput"},
			"B": {Name: "B", Type: "echo"},
		},
		Connections: map[string][]model.Connection{
			"A": {{SourceName: "A", SourceIndex: 0, TargetName: "B", TargetIndex: 0}},
		},
	}
	execA := &alwaysFailExecutor{errMsg: "read failed"}
	execB := &echoExecutor{}
	e := New(map[model.NodeType]NodeExecutor{"alwaysFail": execA, "echo": execB})
	rc := buildTestRunContext(wf)

	ex, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if execB.calls != 1 {
		t.Fatalf("B.calls = %d, want 1 -- the error item from A must still reach B", execB.calls)
	}
	if ex.Status != model.StatusSuccess {
		t.Errorf("status = %s, want success (A's failure was explicitly marked continue-on-fail)", ex.Status)
	}
	// Find A's recorded run and confirm it captured the error AND a
	// visible output item (matching n8n's own continueOnFail item
	// shape), not just a silently-dropped failure.
	var foundA bool
	for _, nr := range ex.NodeRuns {
		if nr.NodeName != "A" {
			continue
		}
		foundA = true
		if nr.Status != model.StatusError {
			t.Errorf("A's recorded status = %s, want error", nr.Status)
		}
		if len(nr.Output) == 0 || len(nr.Output[0]) == 0 {
			t.Fatal("A's recorded output is empty -- error item was not preserved for the execution panel")
		}
		if nr.Output[0][0].JSON["error"] != "read failed" {
			t.Errorf("A's output error field = %v, want %q", nr.Output[0][0].JSON["error"], "read failed")
		}
	}
	if !foundA {
		t.Fatal("no NodeRunResult recorded for A")
	}
}

// TestEngineContinueErrorOutputRoutesToSecondBranch verifies that
// onError=continueErrorOutput sends the error item to the node's
// dedicated second output branch (index 1), not the regular/main
// output (index 0) -- these are genuinely different n8n behaviors.
func TestEngineContinueErrorOutputRoutesToSecondBranch(t *testing.T) {
	wf := &model.Workflow{
		ID: "wf-error-branch",
		Nodes: map[string]*model.Node{
			"A": {Name: "A", Type: "alwaysFail", ContinueOnFail: true, OnErrorMode: "continueErrorOutput"},
			"B": {Name: "B", Type: "echo"}, // regular/main branch -- should NOT run
			"C": {Name: "C", Type: "echo"}, // error branch -- should run
		},
		Connections: map[string][]model.Connection{
			"A": {
				{SourceName: "A", SourceIndex: 0, TargetName: "B", TargetIndex: 0},
				{SourceName: "A", SourceIndex: 1, TargetName: "C", TargetIndex: 0},
			},
		},
	}
	execA := &alwaysFailExecutor{errMsg: "boom"}
	execB := &echoExecutor{}
	execC := &echoExecutor{}
	e := New(map[model.NodeType]NodeExecutor{"alwaysFail": execA, "echo": execB})
	e.Registry["echo"] = execB // keep default registration for type reuse below
	rc := buildTestRunContext(wf)

	// Register B and C separately since both share type "echo" but are
	// different node *instances* -- the engine dispatches by node.Type,
	// so we need distinct types to tell which one actually ran.
	wf.Nodes["C"].Type = "echoC"
	e.Registry["echoC"] = execC

	_, err := e.Run(context.Background(), rc, "A", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if execB.calls != 0 {
		t.Errorf("B (regular branch).calls = %d, want 0 -- continueErrorOutput must not use the main branch", execB.calls)
	}
	if execC.calls != 1 {
		t.Errorf("C (error branch).calls = %d, want 1", execC.calls)
	}
}

func TestRetryBackoffDoublesAndCaps(t *testing.T) {
	if got := retryBackoff(1000, 1); got != 1*time.Second {
		t.Errorf("attempt 1: got %v, want 1s", got)
	}
	if got := retryBackoff(1000, 2); got != 2*time.Second {
		t.Errorf("attempt 2: got %v, want 2s", got)
	}
	if got := retryBackoff(1000, 3); got != 4*time.Second {
		t.Errorf("attempt 3: got %v, want 4s", got)
	}
	if got := retryBackoff(1000, 20); got != maxRetryBackoff {
		t.Errorf("large attempt: got %v, want capped at %v", got, maxRetryBackoff)
	}
	if got := retryBackoff(0, 1); got != 1*time.Second {
		t.Errorf("zero base defaults to 1s: got %v", got)
	}
}
