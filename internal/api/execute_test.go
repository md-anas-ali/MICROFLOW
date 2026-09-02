// Tests for POST /api/workflows/{id}/execute -- in particular the
// silent-failure bug fixed in handleExecute: when runner.RunFromNode
// fails before a run ever starts (e.g. its scratch directory can't be
// created), it returns (nil, err), and the handler used to discard
// err and reply 200 with a JSON `null` body. These tests pin down
// both the normal success path and the fixed error path.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/runner"
)

// fakeStaticData/fakeCreds are the minimal no-op implementations
// runner.New requires; execute tests don't exercise either.
type fakeStaticData struct{}

func (fakeStaticData) WithLock(ctx context.Context, workflowID string, fn func(map[string]any) (map[string]any, error)) error {
	_, err := fn(map[string]any{})
	return err
}

type fakeCreds struct{}

func (fakeCreds) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	return nil, nil
}

// fakeExecSaver records executions so success-path tests can assert
// SaveExecution was actually called (rule: persist win or lose).
type fakeExecSaver struct{ saved []*model.Execution }

func (f *fakeExecSaver) SaveExecution(ctx context.Context, ex *model.Execution) error {
	f.saved = append(f.saved, ex)
	return nil
}

func testWorkflow() *model.Workflow {
	return &model.Workflow{
		ID:   "wf1",
		Name: "Test Workflow",
		Nodes: map[string]*model.Node{
			"Start": {Name: "Start", Type: model.TypeManualTrigger},
		},
		Connections: map[string][]model.Connection{},
	}
}

func TestHandleExecuteHappyPath(t *testing.T) {
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{"wf1": testWorkflow()}}
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{})
	execs := &fakeExecSaver{}
	run := runner.New(wfStore, execs, eng, fakeStaticData{}, fakeCreds{}, t.TempDir())

	s := New(wfStore, run, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleExecute(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() == 0 || rec.Body.String() == "null\n" {
		t.Fatalf("expected a real execution body on success, got: %s", rec.Body.String())
	}
	if len(execs.saved) != 1 {
		t.Fatalf("expected the execution to be persisted, got %d saves", len(execs.saved))
	}
}

// TestHandleExecuteScratchDirFailureReturnsProperError is the
// regression test for the fixed bug: force RunFromNode's scratch-dir
// creation to fail (by pointing ScratchRoot at a path that's a
// *file*, so os.MkdirAll under it always errors) and assert the
// handler now replies with a real HTTP error status and a JSON
// `{"error": ...}` body instead of 200 + `null`.
func TestHandleExecuteScratchDirFailureReturnsProperError(t *testing.T) {
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{"wf1": testWorkflow()}}
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{})
	execs := &fakeExecSaver{}

	// A regular file where a directory is expected: os.MkdirAll(file/x)
	// always fails with ENOTDIR, deterministically reproducing the
	// "RunFromNode fails before a run starts" case without touching
	// real filesystem permissions.
	blockedRoot := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedRoot, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	run := runner.New(wfStore, execs, eng, fakeStaticData{}, fakeCreds{}, blockedRoot)

	s := New(wfStore, run, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleExecute(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected a non-200 error status, got 200 with body: %s", rec.Body.String())
	}
	if rec.Body.String() == "null\n" || rec.Body.Len() == 0 {
		t.Fatalf("expected a JSON error body, got: %q", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("expected valid JSON error body, got %q: %v", rec.Body.String(), err)
	}
	if body["error"] == "" {
		t.Fatalf("expected a non-empty error message, got body: %v", body)
	}
	if len(execs.saved) != 0 {
		t.Fatalf("expected no execution to be persisted when the run never started, got %d", len(execs.saved))
	}
}

// TestExecuteRouteMethodRegression is a routing-level regression test
// (mission section 4/17): the Execute endpoint is registered as
// `POST /api/workflows/{id}/execute` on the real *http.ServeMux built
// by New()/routes() -- not just called directly like the tests above.
// It locks in that GET (and any other non-POST method) on the exact
// same path correctly gets Go 1.22 ServeMux's built-in 405
// MethodNotAllowed, POST on that path is routed to the real handler
// (200, not 404/405), and a neighboring but distinct path/method
// combination is not accidentally matched by the same pattern.
func TestExecuteRouteMethodRegression(t *testing.T) {
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{"wf1": testWorkflow()}}
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{})
	execs := &fakeExecSaver{}
	run := runner.New(wfStore, execs, eng, fakeStaticData{}, fakeCreds{}, t.TempDir())
	s := New(wfStore, run, nil, nil, nil)
	handler := s.Handler()

	t.Run("GET on execute path is 405, not 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/workflows/wf1/execute", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET /api/workflows/wf1/execute: got %d, want %d", rec.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("POST on execute path is routed and succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST /api/workflows/wf1/execute: got %d, want 200, body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("trailing slash does not silently match", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/execute/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			t.Fatalf("POST with trailing slash unexpectedly matched the execute route (got 200)")
		}
	})
}
