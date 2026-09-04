// Tests for GET /api/executions/{id}/debug-report -- the "1-Click Full
// Execution Copy / Debug Report" feature's API surface. The report
// content itself (field derivation, masking, truncation) is covered by
// internal/report's own tests; these tests pin down the HTTP contract:
// status codes, content types, and which query params select which
// response shape.
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"microflow/internal/model"
)

func debugReportTestServer(t *testing.T) (*Server, *fakeExecLoader) {
	t.Helper()
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{
		"wf1": {
			ID:   "wf1",
			Name: "Debug Report Test Workflow",
			Nodes: map[string]*model.Node{
				"A": {ID: "n1", Name: "A", Type: model.TypeManualTrigger},
				"B": {ID: "n2", Name: "B", Type: model.TypeHTTPRequest},
			},
			Connections: map[string][]model.Connection{
				"A": {{SourceName: "A", TargetName: "B"}},
			},
		},
	}}
	execs := &fakeExecLoader{execs: map[string]*model.Execution{}}
	s := New(wfStore, nil, nil, nil, nil)
	s.WithAsync(nil, execs) // manager stays nil -- resolveExecution falls straight to execLoader
	return s, execs
}

func TestHandleDebugReport_JSONDefault(t *testing.T) {
	s, execs := debugReportTestServer(t)
	start := time.Now()
	execs.execs["exec1"] = &model.Execution{
		ID:         "exec1",
		WorkflowID: "wf1",
		Status:     model.StatusSuccess,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{NodeName: "A", Status: model.StatusSuccess, StartedAt: start, Duration: time.Second, Attempt: 1},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/executions/exec1/debug-report", nil)
	req.SetPathValue("id", "exec1")
	rec := httptest.NewRecorder()
	s.handleDebugReport(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
	if disp := rec.Header().Get("Content-Disposition"); !strings.Contains(disp, "attachment") || !strings.Contains(disp, ".json") {
		t.Fatalf("expected a downloadable attachment Content-Disposition, got %q", disp)
	}

	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response body is not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	summary, ok := parsed["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected a summary object in the JSON report, got: %v", parsed)
	}
	if summary["executionId"] != "exec1" {
		t.Fatalf("expected executionId exec1, got %v", summary["executionId"])
	}
	if summary["workflowName"] != "Debug Report Test Workflow" {
		t.Fatalf("expected workflow name enrichment from the workflow store, got %v", summary["workflowName"])
	}
}

func TestHandleDebugReport_FullText(t *testing.T) {
	s, execs := debugReportTestServer(t)
	start := time.Now()
	execs.execs["exec2"] = &model.Execution{
		ID:         "exec2",
		WorkflowID: "wf1",
		Status:     model.StatusError,
		StartedAt:  start,
		Error:      "engine: something went wrong",
		NodeRuns: []model.NodeRunResult{
			{NodeName: "A", Status: model.StatusSuccess, StartedAt: start, Duration: time.Second, Attempt: 1},
			{NodeName: "B", Status: model.StatusError, StartedAt: start.Add(time.Second), Duration: time.Second, Attempt: 1, Error: "http request \"B\": configuration error (HTTP 401): bad key"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/executions/exec2/debug-report?format=text", nil)
	req.SetPathValue("id", "exec2")
	rec := httptest.NewRecorder()
	s.handleDebugReport(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("expected text/plain, got %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "MICROFLOW — FULL EXECUTION REPORT") {
		t.Fatalf("expected full report header, got:\n%s", body)
	}
	if !strings.Contains(body, "NODE 1") || !strings.Contains(body, "NODE 2") {
		t.Fatalf("expected both node blocks in the full report:\n%s", body)
	}
}

func TestHandleDebugReport_ErrorScopeOnlyIncludesFailedNodes(t *testing.T) {
	s, execs := debugReportTestServer(t)
	start := time.Now()
	execs.execs["exec3"] = &model.Execution{
		ID:         "exec3",
		WorkflowID: "wf1",
		Status:     model.StatusError,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{NodeName: "A", Status: model.StatusSuccess, StartedAt: start, Duration: time.Second, Attempt: 1},
			{NodeName: "B", Status: model.StatusError, StartedAt: start.Add(time.Second), Duration: time.Second, Attempt: 1, Error: "boom"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/executions/exec3/debug-report?format=text&scope=error", nil)
	req.SetPathValue("id", "exec3")
	rec := httptest.NewRecorder()
	s.handleDebugReport(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "\"A\"") || strings.Contains(body, "Name: A\n") {
		t.Fatalf("error-scope report should not include the successful node A:\n%s", body)
	}
	if !strings.Contains(body, "Name: B") {
		t.Fatalf("error-scope report should include the failed node B:\n%s", body)
	}
	if !strings.Contains(body, "MICROFLOW — ERROR REPORT") {
		t.Fatalf("expected error report header:\n%s", body)
	}
}

func TestHandleDebugReport_SecretsNeverAppearInAnyFormat(t *testing.T) {
	s, execs := debugReportTestServer(t)
	start := time.Now()
	execs.execs["exec4"] = &model.Execution{
		ID:         "exec4",
		WorkflowID: "wf1",
		Status:     model.StatusError,
		StartedAt:  start,
		NodeRuns: []model.NodeRunResult{
			{
				NodeName:  "B",
				Status:    model.StatusError,
				StartedAt: start,
				Duration:  time.Second,
				Attempt:   1,
				Input: model.NodeOutput{{{JSON: map[string]any{
					"Authorization": "Bearer sk-supersecretvalue123",
				}}}},
				Error: "request failed with TOKEN=sk-supersecretvalue123",
			},
		},
	}

	for _, url := range []string{
		"/api/executions/exec4/debug-report",
		"/api/executions/exec4/debug-report?format=text",
		"/api/executions/exec4/debug-report?format=text&scope=error",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req.SetPathValue("id", "exec4")
		rec := httptest.NewRecorder()
		s.handleDebugReport(rec, req)
		if strings.Contains(rec.Body.String(), "sk-supersecretvalue123") {
			t.Fatalf("secret leaked into response for %s:\n%s", url, rec.Body.String())
		}
	}
}

func TestHandleDebugReport_UnknownExecutionReturns404(t *testing.T) {
	s, _ := debugReportTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/executions/does-not-exist/debug-report", nil)
	req.SetPathValue("id", "does-not-exist")
	rec := httptest.NewRecorder()
	s.handleDebugReport(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown execution, got %d", rec.Code)
	}
}
