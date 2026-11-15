package mcp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const tok = "0123456789abcdef-test-token"

// fakeAPI mimics the slice of /api/* the tools call.
func fakeAPI() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/workflows", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"w1","name":"Shorts","serviceId":"default","active":true,
			"nodes":{"A":{"name":"A","type":"code","parameters":{"secret":"SHOULD-NOT-LEAK"}}}}]`))
	})
	mux.HandleFunc("GET /api/workflows/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Microflow-Service") == "other" {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"workflow not found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"w1","name":"Shorts","serviceId":"default","active":true,
			"nodes":{"A":{"name":"A","type":"code","parameters":{"secret":"SHOULD-NOT-LEAK"}}},
			"connections":{"A":[{"sourceName":"A","sourceIndex":0,"targetName":"B","targetIndex":0}]}}`))
	})
	mux.HandleFunc("POST /api/workflows/{id}/execute", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"executionId":"e1","status":"queued"}`))
	})
	mux.HandleFunc("GET /api/executions", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"executions":[
			{"id":"e1","workflowId":"w1","status":"error","error":"boom","mode":"manual"},
			{"id":"e2","workflowId":"w2","status":"success","mode":"manual"}],"queue":{"queued":0}}`))
	})
	mux.HandleFunc("GET /api/executions/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"e1","workflowId":"w1","status":"error","error":"boom",
			"nodeRuns":[{"nodeName":"A","status":"error","error":"tts failed","attempt":1,"durationMs":2500000000}]}`))
	})
	mux.HandleFunc("GET /api/executions/{id}/debug-report", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "text" {
			w.WriteHeader(400)
			return
		}
		_, _ = w.Write([]byte("REPORT scope=" + r.URL.Query().Get("scope")))
	})
	mux.HandleFunc("POST /api/executions/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"canceling"}`))
	})
	return mux
}

func post(t *testing.T, h http.Handler, auth, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func callTool(t *testing.T, h http.Handler, name, argsJSON string) (text string, isErr bool) {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":` + argsJSON + `}}`
	rec := post(t, h, "Bearer "+tok, body)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp struct {
		Result struct {
			Content []struct{ Text string } `json:"content"`
			IsError bool                    `json:"isError"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Result.Content) == 0 {
		t.Fatalf("bad response: %s", rec.Body)
	}
	return resp.Result.Content[0].Text, resp.Result.IsError
}

func TestAuth(t *testing.T) {
	h := NewHandler(fakeAPI(), Config{Token: tok})
	for _, a := range []string{"", "Bearer wrong", "Basic " + tok, "Bearer "} {
		if rec := post(t, h, a, `{}`); rec.Code != 401 {
			t.Errorf("auth %q: got %d want 401", a, rec.Code)
		}
	}
	if rec := post(t, h, "Bearer "+tok, `{"jsonrpc":"2.0","id":1,"method":"ping"}`); rec.Code != 200 {
		t.Errorf("valid token: got %d", rec.Code)
	}
}

func TestConfigRejectsShortToken(t *testing.T) {
	t.Setenv("MICROFLOW_MCP_TOKEN", "short")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("short token must be rejected")
	}
	t.Setenv("MICROFLOW_MCP_TOKEN", "")
	c, err := ConfigFromEnv()
	if err != nil || c.Enabled() {
		t.Fatal("empty token must mean disabled, no error")
	}
}

func TestInitializeAndNotification(t *testing.T) {
	h := NewHandler(fakeAPI(), Config{Token: tok})
	rec := post(t, h, "Bearer "+tok, `{"jsonrpc":"2.0","id":"x","method":"initialize","params":{"protocolVersion":"2025-03-26"}}`)
	if !strings.Contains(rec.Body.String(), `"protocolVersion":"2025-03-26"`) || !strings.Contains(rec.Body.String(), `"tools"`) {
		t.Fatalf("bad initialize: %s", rec.Body)
	}
	rec = post(t, h, "Bearer "+tok, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if rec.Code != 202 || rec.Body.Len() != 0 {
		t.Fatalf("notification: %d %q", rec.Code, rec.Body)
	}
	rec = post(t, h, "Bearer "+tok, `{"jsonrpc":"2.0","id":2,"method":"nope"}`)
	if !strings.Contains(rec.Body.String(), "-32601") {
		t.Fatalf("unknown method: %s", rec.Body)
	}
}

func TestToolsListReadOnly(t *testing.T) {
	rw := NewHandler(fakeAPI(), Config{Token: tok})
	ro := NewHandler(fakeAPI(), Config{Token: tok, ReadOnly: true})
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	if s := post(t, rw, "Bearer "+tok, body).Body.String(); !strings.Contains(s, "run_workflow") || !strings.Contains(s, "cancel_execution") {
		t.Fatalf("write tools missing: %s", s)
	}
	s := post(t, ro, "Bearer "+tok, body).Body.String()
	if strings.Contains(s, "run_workflow") || strings.Contains(s, "cancel_execution") {
		t.Fatalf("read-only must hide write tools: %s", s)
	}
	text, isErr := callTool2(t, ro, "run_workflow", `{"workflowId":"w1"}`)
	if !isErr || !strings.Contains(text, "unknown tool") {
		t.Fatalf("read-only must refuse run_workflow, got %q err=%v", text, isErr)
	}
}

func callTool2(t *testing.T, h http.Handler, name, args string) (string, bool) {
	return callTool(t, h, name, args)
}

func TestTools(t *testing.T) {
	h := NewHandler(fakeAPI(), Config{Token: tok})

	text, isErr := callTool(t, h, "list_workflows", `{}`)
	if isErr || !strings.Contains(text, `"nodeCount": 1`) || strings.Contains(text, "SHOULD-NOT-LEAK") {
		t.Fatalf("list_workflows: %q", text)
	}
	text, isErr = callTool(t, h, "get_workflow", `{"workflowId":"w1"}`)
	if isErr || !strings.Contains(text, `"to": "B"`) || strings.Contains(text, "SHOULD-NOT-LEAK") {
		t.Fatalf("get_workflow: %q", text)
	}
	text, isErr = callTool(t, h, "get_workflow", `{"workflowId":"w1","serviceId":"other"}`)
	if !isErr || !strings.Contains(text, "404") {
		t.Fatalf("service isolation must surface as error: %q", text)
	}
	text, isErr = callTool(t, h, "list_executions", `{"status":"error"}`)
	if isErr || !strings.Contains(text, `"e1"`) || strings.Contains(text, `"e2"`) {
		t.Fatalf("list_executions filter: %q", text)
	}
	text, isErr = callTool(t, h, "get_execution", `{"executionId":"e1"}`)
	if isErr || !strings.Contains(text, "tts failed") || !strings.Contains(text, `"durationMs": 2500`) {
		t.Fatalf("get_execution: %q", text)
	}
	text, isErr = callTool(t, h, "get_execution_report", `{"executionId":"e1","scope":"full"}`)
	if isErr || text != "REPORT scope=full" {
		t.Fatalf("report: %q", text)
	}
	text, isErr = callTool(t, h, "run_workflow", `{"workflowId":"w1"}`)
	if isErr || !strings.Contains(text, `"executionId":"e1"`) {
		t.Fatalf("run_workflow: %q", text)
	}
	text, isErr = callTool(t, h, "cancel_execution", `{"executionId":"e1"}`)
	if isErr || !strings.Contains(text, "canceling") {
		t.Fatalf("cancel: %q", text)
	}
	text, isErr = callTool(t, h, "get_execution", `{}`)
	if !isErr || !strings.Contains(text, "executionId is required") {
		t.Fatalf("missing arg: %q", text)
	}
}

func TestGetNotAllowed(t *testing.T) {
	h := NewHandler(fakeAPI(), Config{Token: tok})
	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 405 {
		t.Fatalf("GET: %d", rec.Code)
	}
}
