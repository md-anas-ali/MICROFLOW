// Tests for the async execution endpoints (handleExecuteAsync,
// handleGetExecution, handleCancelExecution, handleExecutionEvents),
// added alongside internal/runner's Manager. Deliberately separate
// from execute_test.go, which pins down the *synchronous* fallback
// path (a Server built with plain New(), no WithAsync) and must keep
// passing unchanged.
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/runner"
)

type instantExecutor struct{}

func (instantExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	return model.NodeOutput{{{JSON: map[string]any{"ok": true}}}}, nil
}

type blockingExecutor struct{ release <-chan struct{} }

func (b blockingExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	select {
	case <-b.release:
		return model.NodeOutput{{{JSON: map[string]any{"ok": true}}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fakeExecLoader is the ExecutionLoader fallback -- these tests keep
// it empty on purpose so a hit always comes from the live Manager, not
// this fallback, unless a test explicitly seeds it.
type fakeExecLoader struct{ execs map[string]*model.Execution }

func (f *fakeExecLoader) GetExecution(ctx context.Context, id string) (*model.Execution, error) {
	if ex, ok := f.execs[id]; ok {
		return ex, nil
	}
	return nil, errString("not found")
}

func (f *fakeExecLoader) ListExecutions(ctx context.Context, limit int) ([]*model.Execution, error) {
	out := make([]*model.Execution, 0, len(f.execs))
	for _, ex := range f.execs {
		out = append(out, ex)
	}
	return out, nil
}

type errString string

func (e errString) Error() string { return string(e) }

func newAsyncTestServer(t *testing.T, registry map[model.NodeType]engine.NodeExecutor, maxConcurrent, maxQueued int) (*Server, *fakeWorkflowStore) {
	t.Helper()
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{"wf1": testWorkflow()}}
	eng := engine.New(registry)
	execs := &fakeExecSaver{}
	run := runner.New(wfStore, execs, eng, fakeStaticData{}, fakeCreds{}, t.TempDir())
	run.Timeout = 5 * time.Second
	mgr := runner.NewManager(run, maxConcurrent, maxQueued)
	s := New(wfStore, run, nil, nil, nil).WithAsync(mgr, &fakeExecLoader{execs: map[string]*model.Execution{}})
	return s, wfStore
}

func TestHandleExecuteAsyncReturns202Immediately(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, 2, 5)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	start := time.Now()
	s.handleExecute(rec, req)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("handleExecute took %v -- should return before the run finishes", elapsed)
	}

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var body executeAcceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ExecutionID == "" {
		t.Fatal("expected a non-empty executionId")
	}
	if body.Status != "queued" {
		t.Fatalf("status = %q, want queued", body.Status)
	}
}

func TestHandleGetExecutionReflectsCompletion(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, 2, 5)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()
	s.handleExecute(rec, req)
	var accepted executeAcceptedResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &accepted)

	deadline := time.Now().Add(2 * time.Second)
	var ex model.Execution
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/api/executions/"+accepted.ExecutionID, nil)
		getReq.SetPathValue("id", accepted.ExecutionID)
		getRec := httptest.NewRecorder()
		s.handleGetExecution(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("GET execution: %d: %s", getRec.Code, getRec.Body.String())
		}
		_ = json.Unmarshal(getRec.Body.Bytes(), &ex)
		if ex.Status == model.StatusSuccess || ex.Status == model.StatusError {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ex.Status != model.StatusSuccess {
		t.Fatalf("final status = %q, want success (error=%q)", ex.Status, ex.Error)
	}
}

func TestHandleListExecutionsIncludesLiveQueueStats(t *testing.T) {
	release := make(chan struct{})
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: blockingExecutor{release: release}}, 1, 2)
	defer close(release)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
		req.SetPathValue("id", "wf1")
		rec := httptest.NewRecorder()
		s.handleExecute(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("execute %d: got %d: %s", i+1, rec.Code, rec.Body.String())
		}
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/api/executions?limit=10", nil)
		rec := httptest.NewRecorder()
		s.handleListExecutions(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list: %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Executions []model.Execution `json:"executions"`
			Queue      map[string]int    `json:"queue"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		if len(body.Executions) >= 2 && body.Queue["accepted"] == 2 && body.Queue["running"] == 1 && body.Queue["waiting"] == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("list never exposed the expected live queue state")
}

func TestHandleCancelExecution(t *testing.T) {
	release := make(chan struct{})
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: blockingExecutor{release: release}}, 1, 2)
	defer close(release)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()
	s.handleExecute(rec, req)
	var accepted executeAcceptedResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &accepted)

	// Give it a moment to actually enter StatusRunning before cancelling.
	time.Sleep(30 * time.Millisecond)

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/executions/"+accepted.ExecutionID+"/cancel", nil)
	cancelReq.SetPathValue("id", accepted.ExecutionID)
	cancelRec := httptest.NewRecorder()
	s.handleCancelExecution(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("cancel: %d: %s", cancelRec.Code, cancelRec.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	var ex model.Execution
	for time.Now().Before(deadline) {
		getReq := httptest.NewRequest(http.MethodGet, "/api/executions/"+accepted.ExecutionID, nil)
		getReq.SetPathValue("id", accepted.ExecutionID)
		getRec := httptest.NewRecorder()
		s.handleGetExecution(getRec, getReq)
		_ = json.Unmarshal(getRec.Body.Bytes(), &ex)
		if ex.Status == model.StatusCancelled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("execution never reached StatusCancelled (last status=%q)", ex.Status)
}

func TestHandleCancelExecutionUnknownID(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, 1, 1)

	req := httptest.NewRequest(http.MethodPost, "/api/executions/does-not-exist/cancel", nil)
	req.SetPathValue("id", "does-not-exist")
	rec := httptest.NewRecorder()
	s.handleCancelExecution(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown execution id, got %d", rec.Code)
	}
}

// TestHandleExecutionEventsStreamsToTerminalEvent drives the SSE
// handler directly (httptest.ResponseRecorder implements http.Flusher)
// with a request context that's cancelled once we've seen a terminal
// event or a timeout elapses, then parses the raw SSE body for at
// least one `data: {"type":"execution.completed"...}` line -- proving
// events actually flow over this endpoint's wire format, not just
// through the in-process broadcaster (already covered by
// internal/runner's own tests).
// syncRecorder wraps httptest.ResponseRecorder with a mutex so a test
// goroutine can safely poll the response body while a concurrently
// running handler goroutine is still writing to it (SSE handlers, by
// nature, write across the lifetime of the request instead of once at
// the end) -- a plain httptest.ResponseRecorder is not safe for that
// concurrent access pattern, which -race correctly flags.
type syncRecorder struct {
	mu  sync.Mutex
	rec *httptest.ResponseRecorder
}

func newSyncRecorder() *syncRecorder {
	return &syncRecorder{rec: httptest.NewRecorder()}
}

func (s *syncRecorder) Header() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Header()
}

func (s *syncRecorder) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Write(b)
}

func (s *syncRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.WriteHeader(code)
}

func (s *syncRecorder) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.Flush()
}

func (s *syncRecorder) body() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Body.String()
}

func (s *syncRecorder) code() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Code
}

func (s *syncRecorder) contentType() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Header().Get("Content-Type")
}

func TestHandleCancelExecutionAlreadyFinishedIsIdempotent(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, 1, 1)
	loader := &fakeExecLoader{execs: map[string]*model.Execution{
		"done-1": {ID: "done-1", WorkflowID: "wf1", Mode: "manual", Status: model.StatusSuccess, StartedAt: time.Now()},
	}}
	s.execLoader = loader

	req := httptest.NewRequest(http.MethodPost, "/api/executions/done-1/cancel", nil)
	req.SetPathValue("id", "done-1")
	rec := httptest.NewRecorder()
	s.handleCancelExecution(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for already-finished execution, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleExecutionEventsStreamsToTerminalEvent(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}}, 2, 5)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()
	s.handleExecute(rec, req)
	var accepted executeAcceptedResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &accepted)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	evReq := httptest.NewRequest(http.MethodGet, "/api/executions/"+accepted.ExecutionID+"/events", nil)
	evReq = evReq.WithContext(ctx)
	evReq.SetPathValue("id", accepted.ExecutionID)
	evRec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		s.handleExecutionEvents(evRec, evReq)
		close(done)
	}()

	// Poll the recorder's buffered body for a terminal event, then
	// cancel the request context so the handler returns (it otherwise
	// only returns on ctx.Done or a closed broadcaster -- both of which
	// this satisfies once the run finishes and we cancel).
	deadline := time.Now().Add(2 * time.Second)
	sawTerminal := false
	for time.Now().Before(deadline) {
		if strings.Contains(evRec.body(), `"type":"execution.completed"`) {
			sawTerminal = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	body := evRec.body()
	if !sawTerminal {
		t.Fatalf("never saw an execution.completed SSE event; body:\n%s", body)
	}
	if code := evRec.code(); code != http.StatusOK {
		t.Fatalf("SSE response code = %d, want 200", code)
	}
	if ct := evRec.contentType(); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Sanity-check the wire format itself: at least one well-formed
	// "data: {...}\n\n" SSE frame decodable back into runner.Event.
	scanner := bufio.NewScanner(strings.NewReader(body))
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev runner.Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			t.Fatalf("malformed SSE data line %q: %v", line, err)
		}
		found = true
	}
	if !found {
		t.Fatal("no data: lines found in SSE body")
	}
}

func TestHandleExecutionEventsUnknownID(t *testing.T) {
	s, _ := newAsyncTestServer(t, map[model.NodeType]engine.NodeExecutor{}, 1, 1)

	req := httptest.NewRequest(http.MethodGet, "/api/executions/does-not-exist/events", nil)
	req.SetPathValue("id", "does-not-exist")
	rec := httptest.NewRecorder()
	s.handleExecutionEvents(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown execution id, got %d", rec.Code)
	}
}

// TestHandleExecuteSyncFallbackUnaffected pins down that a Server
// built without WithAsync (execute_test.go's exact pattern) still
// behaves exactly as before -- this new file must not have broken it.
func TestHandleExecuteSyncFallbackUnaffected(t *testing.T) {
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{"wf1": testWorkflow()}}
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: instantExecutor{}})
	execs := &fakeExecSaver{}
	run := runner.New(wfStore, execs, eng, fakeStaticData{}, fakeCreds{}, t.TempDir())
	s := New(wfStore, run, nil, nil, nil) // no WithAsync

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()
	s.handleExecute(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (sync fallback), got %d: %s", rec.Code, rec.Body.String())
	}
	var ex model.Execution
	if err := json.Unmarshal(rec.Body.Bytes(), &ex); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ex.Status != model.StatusSuccess {
		t.Fatalf("status = %q, want success", ex.Status)
	}
}
