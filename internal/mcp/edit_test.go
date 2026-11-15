package mcp

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// statefulAPI is a tiny in-memory stand-in for GET/POST /api/workflows/{id}.
type statefulAPI struct {
	mu    sync.Mutex
	doc   string
	saves int
}

func (s *statefulAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/workflows/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Microflow-Service") == "other" {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"workflow not found"}`))
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		_, _ = w.Write([]byte(s.doc))
	})
	mux.HandleFunc("POST /api/workflows/{id}/save", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !json.Valid(b) {
			w.WriteHeader(400)
			return
		}
		s.mu.Lock()
		s.doc = string(b)
		s.saves++
		s.mu.Unlock()
		_, _ = w.Write(b)
	})
	return mux
}

const baseDoc = `{"id":"w1","name":"Shorts","serviceId":"default","active":true,"settings":{"x":1},
 "nodes":{
  "Fetch":{"id":"n1","name":"Fetch","type":"httpRequest","typeVersion":4.2,"position":[10,20],
    "parameters":{"url":"https://a.test","timeout":30000,"headers":{"Authorization":"Bearer REAL-SECRET","X-Ok":"1"}},
    "retryOnFail":false,"maxTries":0,"waitBetweenTriesMs":0,"disabled":false},
  "Other":{"id":"n2","name":"Other","type":"code","parameters":{"jsCode":"return items"}}},
 "connections":{"Fetch":[{"sourceName":"Fetch","sourceIndex":0,"targetName":"Other","targetIndex":0}]}}`

func editHandler(cfg Config) (*Handler, *statefulAPI) {
	api := &statefulAPI{doc: baseDoc}
	cfg.Token = tok
	return NewHandler(api.handler(), cfg), api
}

func TestEditToolsHiddenByDefault(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	for name, cfg := range map[string]Config{
		"default":        {Token: tok},
		"readonly+allow": {Token: tok, ReadOnly: true, AllowEdit: true},
	} {
		h := NewHandler(fakeAPI(), cfg)
		s := post(t, h, "Bearer "+tok, body).Body.String()
		if strings.Contains(s, "update_node") || strings.Contains(s, "revert_last_edit") {
			t.Fatalf("%s: edit tools must be hidden: %s", name, s)
		}
		text, isErr := callTool(t, h, "update_node", `{"workflowId":"w1","nodeName":"A","disabled":true}`)
		if !isErr || !strings.Contains(text, "unknown tool") {
			t.Fatalf("%s: update_node must be refused, got %q", name, text)
		}
	}
	h := NewHandler(fakeAPI(), Config{Token: tok, AllowEdit: true})
	s := post(t, h, "Bearer "+tok, body).Body.String()
	for _, n := range []string{"get_node", "update_node", "revert_last_edit"} {
		if !strings.Contains(s, n) {
			t.Fatalf("AllowEdit must expose %s: %s", n, s)
		}
	}
}

func TestConfigAllowEditEnv(t *testing.T) {
	t.Setenv("MICROFLOW_MCP_TOKEN", tok)
	c, _ := ConfigFromEnv()
	if c.AllowEdit {
		t.Fatal("AllowEdit must default to false")
	}
	t.Setenv("MICROFLOW_MCP_ALLOW_EDIT", "true")
	c, _ = ConfigFromEnv()
	if !c.AllowEdit {
		t.Fatal("AllowEdit should be true")
	}
}

func TestGetNodeMasksSecrets(t *testing.T) {
	h, _ := editHandler(Config{AllowEdit: true})
	text, isErr := callTool(t, h, "get_node", `{"workflowId":"w1","nodeName":"Fetch"}`)
	if isErr || strings.Contains(text, "REAL-SECRET") || !strings.Contains(text, redactMask) || !strings.Contains(text, `"X-Ok": "1"`) {
		t.Fatalf("get_node: %q", text)
	}
	text, isErr = callTool(t, h, "get_node", `{"workflowId":"w1","nodeName":"fetc"}`)
	if !isErr || !strings.Contains(text, `similar: "Fetch"`) {
		t.Fatalf("expected similar-name hint, got %q", text)
	}
}

func TestUpdateNodeMergesAndPreservesRest(t *testing.T) {
	h, api := editHandler(Config{AllowEdit: true})

	// dry run saves nothing
	text, isErr := callTool(t, h, "update_node",
		`{"workflowId":"w1","nodeName":"Fetch","parameters":{"timeout":60000},"dryRun":true}`)
	if isErr || api.saves != 0 || !strings.Contains(text, `"dryRun": true`) {
		t.Fatalf("dry run: saves=%d %q", api.saves, text)
	}

	text, isErr = callTool(t, h, "update_node",
		`{"workflowId":"w1","nodeName":"Fetch","parameters":{"timeout":60000,"url":null},"retryOnFail":true,"maxTries":3,"waitBetweenTriesMs":5000}`)
	if isErr || api.saves != 1 || !strings.Contains(text, `"saved": true`) {
		t.Fatalf("update: saves=%d err=%v %q", api.saves, isErr, text)
	}
	if strings.Contains(text, "REAL-SECRET") {
		t.Fatalf("diff must not leak secrets: %q", text)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(api.doc), &got); err != nil {
		t.Fatal(err)
	}
	n := got["nodes"].(map[string]any)["Fetch"].(map[string]any)
	p := n["parameters"].(map[string]any)
	if p["timeout"].(float64) != 60000 {
		t.Errorf("timeout not merged: %v", p)
	}
	if _, has := p["url"]; has {
		t.Errorf("null must delete key: %v", p)
	}
	if p["headers"].(map[string]any)["Authorization"] != "Bearer REAL-SECRET" {
		t.Errorf("untouched secret must survive byte-for-byte: %v", p["headers"])
	}
	if n["retryOnFail"] != true || n["maxTries"].(float64) != 3 || n["waitBetweenTriesMs"].(float64) != 5000 {
		t.Errorf("retry settings: %v", n)
	}
	if n["typeVersion"].(float64) != 4.2 {
		t.Errorf("typeVersion changed: %v", n["typeVersion"])
	}
	if got["settings"].(map[string]any)["x"].(float64) != 1 || got["serviceId"] != "default" {
		t.Errorf("rest of workflow not preserved: %v", got)
	}
	if _, ok := got["nodes"].(map[string]any)["Other"]; !ok {
		t.Error("other node lost")
	}
}

func TestUpdateNodeRefusesMaskAndBadInput(t *testing.T) {
	h, api := editHandler(Config{AllowEdit: true})
	cases := map[string]string{
		"mask":        `{"workflowId":"w1","nodeName":"Fetch","parameters":{"headers":{"Authorization":"` + redactMask + `"}}}`,
		"empty":       `{"workflowId":"w1","nodeName":"Fetch"}`,
		"maxTries":    `{"workflowId":"w1","nodeName":"Fetch","maxTries":99}`,
		"onError":     `{"workflowId":"w1","nodeName":"Fetch","onErrorMode":"explode"}`,
		"missingNode": `{"workflowId":"w1","nodeName":"Nope","disabled":true}`,
		"isolation":   `{"workflowId":"w1","nodeName":"Fetch","disabled":true,"serviceId":"other"}`,
	}
	for name, args := range cases {
		text, isErr := callTool(t, h, "update_node", args)
		if !isErr {
			t.Errorf("%s: expected error, got %q", name, text)
		}
	}
	if api.saves != 0 {
		t.Fatalf("no bad call may save, saves=%d", api.saves)
	}
}

func TestNoopUpdateDoesNotSave(t *testing.T) {
	h, api := editHandler(Config{AllowEdit: true})
	text, isErr := callTool(t, h, "update_node", `{"workflowId":"w1","nodeName":"Fetch","retryOnFail":false}`)
	if isErr || api.saves != 0 || !strings.Contains(text, "no change") {
		t.Fatalf("noop: saves=%d %q", api.saves, text)
	}
}

func TestRevertLastEdit(t *testing.T) {
	h, api := editHandler(Config{AllowEdit: true})
	if text, isErr := callTool(t, h, "revert_last_edit", `{"workflowId":"w1"}`); !isErr || !strings.Contains(text, "nothing to revert") {
		t.Fatalf("empty revert: %q", text)
	}
	callTool(t, h, "update_node", `{"workflowId":"w1","nodeName":"Fetch","disabled":true}`)
	callTool(t, h, "update_node", `{"workflowId":"w1","nodeName":"Fetch","maxTries":5}`)

	text, isErr := callTool(t, h, "revert_last_edit", `{"workflowId":"w1"}`)
	if isErr || !strings.Contains(text, `"remainingUndo": 1`) {
		t.Fatalf("revert 1: %q", text)
	}
	if strings.Contains(api.doc, `"maxTries":5`) || !strings.Contains(api.doc, `"disabled":true`) {
		t.Fatalf("first revert should undo only the second edit: %s", api.doc)
	}
	if _, isErr := callTool(t, h, "revert_last_edit", `{"workflowId":"w1"}`); isErr {
		t.Fatal("revert 2 failed")
	}
	if api.doc != baseDoc {
		t.Fatalf("full revert must restore the original document exactly:\n%s", api.doc)
	}
	if text, isErr := callTool(t, h, "revert_last_edit", `{"workflowId":"w1"}`); !isErr {
		t.Fatalf("history exhausted, got %q", text)
	}
}
