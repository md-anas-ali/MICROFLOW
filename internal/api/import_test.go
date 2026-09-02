package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/runner"
)

func TestImportedWorkflowCanBeOpenedAfterward(t *testing.T) {
	store := &fakeWorkflowStore{workflows: map[string]*model.Workflow{}}
	run := runner.New(store, &fakeExecSaver{}, engine.New(map[model.NodeType]engine.NodeExecutor{}), fakeStaticData{}, fakeCreds{}, t.TempDir())
	s := New(store, run, nil, nil, nil)

	raw := []byte(`{"name":"My workflow 14","nodes":[{"id":"n1","name":"Start","type":"n8n-nodes-base.manualTrigger"}],"connections":{}}`)
	importReq := httptest.NewRequest(http.MethodPost, "/api/workflows/import", bytes.NewReader(raw))
	importRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(importRec, importReq)
	if importRec.Code != http.StatusOK {
		t.Fatalf("import: got %d, body: %s", importRec.Code, importRec.Body.String())
	}
	var body struct {
		Workflow model.Workflow `json:"workflow"`
	}
	if err := json.Unmarshal(importRec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode import response: %v", err)
	}
	if body.Workflow.ID == "" {
		t.Fatal("import returned an empty workflow ID")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/workflows/"+body.Workflow.ID, nil)
	getRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("open imported workflow: got %d, body: %s", getRec.Code, getRec.Body.String())
	}
}

func TestExecuteAfterImportUsesRealRouting(t *testing.T) {
	store := &fakeWorkflowStore{workflows: map[string]*model.Workflow{}}
	run := runner.New(store, &fakeExecSaver{}, engine.New(map[model.NodeType]engine.NodeExecutor{}), fakeStaticData{}, fakeCreds{}, t.TempDir())
	s := New(store, run, nil, nil, nil)

	raw := []byte(`{"name":"My workflow 14","nodes":[{"id":"n1","name":"Start","type":"n8n-nodes-base.manualTrigger"}],"connections":{}}`)
	importReq := httptest.NewRequest(http.MethodPost, "/api/workflows/import", bytes.NewReader(raw))
	importRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(importRec, importReq)
	if importRec.Code != http.StatusOK {
		t.Fatalf("import: got %d, body: %s", importRec.Code, importRec.Body.String())
	}
	var body struct {
		Workflow model.Workflow `json:"workflow"`
	}
	if err := json.Unmarshal(importRec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode import response: %v", err)
	}
	if body.Workflow.ID == "" {
		t.Fatal("import returned an empty workflow ID")
	}

	execReq := httptest.NewRequest(http.MethodPost, "/api/workflows/"+body.Workflow.ID+"/execute", nil)
	execRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(execRec, execReq)
	if execRec.Code == http.StatusNotFound || execRec.Code == http.StatusMethodNotAllowed || execRec.Code == http.StatusMovedPermanently {
		t.Fatalf("execute route did not accept the imported workflow ID: got %d, body: %s", execRec.Code, execRec.Body.String())
	}
}
