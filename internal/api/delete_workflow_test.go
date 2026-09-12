package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/runner"
	"microflow/internal/store"
)

// mockWorkflowStore is a minimal in-memory WorkflowStore for exercising
// handleDeleteWorkflow's 200/404/409/500 branches without a real
// Postgres instance.
type mockWorkflowStore struct {
	mu  sync.Mutex
	wfs map[string]*model.Workflow

	// deleteErr, if set, is returned by DeleteWorkflow instead of the
	// default not-found/success behavior -- lets a single test force
	// store.ErrWorkflowHasActiveExecutions or a generic failure.
	deleteErr error
}

func newMockWorkflowStore() *mockWorkflowStore {
	return &mockWorkflowStore{wfs: map[string]*model.Workflow{}}
}

func (m *mockWorkflowStore) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wfs[wf.ID] = wf
	return nil
}

func (m *mockWorkflowStore) LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wf, ok := m.wfs[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return wf, nil
}

func (m *mockWorkflowStore) ListWorkflows(ctx context.Context) ([]*model.Workflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*model.Workflow, 0, len(m.wfs))
	for _, wf := range m.wfs {
		out = append(out, wf)
	}
	return out, nil
}

func (m *mockWorkflowStore) DeleteWorkflow(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if _, ok := m.wfs[id]; !ok {
		return store.ErrWorkflowNotFound
	}
	delete(m.wfs, id)
	return nil
}

type mockCredStore struct{}

func (mockCredStore) ListCredentials(ctx context.Context, workflowID string) ([]store.CredentialInfo, error) {
	return nil, nil
}

// blockingExecutor lets a test deterministically hold a workflow "in
// flight" (so runner.Runner.IsWorkflowActive reports true) before
// releasing it -- mirrors internal/runner's own test helper, kept as a
// separate small copy here since it's package-private there.
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

type noopExecs struct{}

func (noopExecs) SaveExecution(ctx context.Context, ex *model.Execution) error { return nil }

func testWorkflow(id string) *model.Workflow {
	return &model.Workflow{
		ID:   id,
		Name: "test",
		Nodes: map[string]*model.Node{
			"Start": {ID: "1", Name: "Start", Type: model.TypeManualTrigger},
		},
	}
}

func newTestServer(t *testing.T) (*Server, *mockWorkflowStore, *runner.Runner, *blockingExecutor) {
	t.Helper()
	exec := newBlockingExecutor()
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{model.TypeManualTrigger: exec})
	wfs := newMockWorkflowStore()
	r := runner.New(wfs, noopExecs{}, eng, nil, nil, t.TempDir())
	r.Timeout = 5 * time.Second
	s := New(wfs, r, mockCredStore{}, nil, nil)
	return s, wfs, r, exec
}

func doDelete(s *Server, id string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/api/workflows/"+id, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

func TestHandleDeleteWorkflow_Success(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wfs.wfs["wf-1"] = testWorkflow("wf-1")

	rr := doDelete(s, "wf-1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" || body["id"] != "wf-1" {
		t.Fatalf("unexpected body: %+v", body)
	}
	if _, err := wfs.LoadWorkflow(context.Background(), "wf-1"); err == nil {
		t.Fatal("workflow should no longer be loadable after delete")
	}
}

func TestHandleDeleteWorkflow_NotFound(t *testing.T) {
	s, _, _, _ := newTestServer(t)

	rr := doDelete(s, "does-not-exist")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleDeleteWorkflow_DoubleDeleteIsIdempotentSafe(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wfs.wfs["wf-1"] = testWorkflow("wf-1")

	first := doDelete(s, "wf-1")
	if first.Code != http.StatusOK {
		t.Fatalf("first delete status = %d, want 200", first.Code)
	}
	second := doDelete(s, "wf-1")
	if second.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want 404 (already gone)", second.Code)
	}
}

// TestHandleDeleteWorkflow_StoreReportsActiveExecutions covers the
// store's own transactional 409 (store.ErrWorkflowHasActiveExecutions),
// independent of the in-memory Runner check.
func TestHandleDeleteWorkflow_StoreReportsActiveExecutions(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wfs.wfs["wf-1"] = testWorkflow("wf-1")
	wfs.deleteErr = store.ErrWorkflowHasActiveExecutions

	rr := doDelete(s, "wf-1")
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
}

// TestHandleDeleteWorkflow_GenericStoreErrorIs500 covers the default
// branch: an unexpected store error must not leak internals and must
// map to 500, not a client error.
func TestHandleDeleteWorkflow_GenericStoreErrorIs500(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wfs.wfs["wf-1"] = testWorkflow("wf-1")
	wfs.deleteErr = errors.New("boom: connection reset")

	rr := doDelete(s, "wf-1")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rr.Code, rr.Body.String())
	}
	if bodyContains(rr.Body.String(), "boom") {
		t.Fatalf("raw store error leaked to client: %s", rr.Body.String())
	}
}

// TestHandleDeleteWorkflow_BlockedWhileRunning is the core safety test:
// a workflow with a live execution in flight (via RunFromNode, the
// synchronous scheduler/webhook path that writes no Postgres row until
// it finishes) must be rejected with 409 by the fast in-memory check,
// and become deletable again the instant the run finishes.
func TestHandleDeleteWorkflow_BlockedWhileRunning(t *testing.T) {
	s, wfs, r, exec := newTestServer(t)
	wfs.wfs["wf-1"] = testWorkflow("wf-1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = r.RunFromNode(context.Background(), "wf-1", "Start", "manual", model.NodeOutput{{{JSON: map[string]any{}}}})
	}()

	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("run never reached the blocking node")
	}

	rr := doDelete(s, "wf-1")
	if rr.Code != http.StatusConflict {
		t.Fatalf("status while running = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
	// The workflow must still exist -- a 409 must never have deleted it.
	if _, err := wfs.LoadWorkflow(context.Background(), "wf-1"); err != nil {
		t.Fatal("workflow should NOT have been deleted while a run was active")
	}

	close(exec.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run never finished after release")
	}

	rr2 := doDelete(s, "wf-1")
	if rr2.Code != http.StatusOK {
		t.Fatalf("status after run finished = %d, want 200; body=%s", rr2.Code, rr2.Body.String())
	}
}

func bodyContains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
