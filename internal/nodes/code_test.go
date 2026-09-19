package nodes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

func newCodeTestRunContext() *engine.RunContext {
	rc := newTestRunContext()
	rc.Workflow = &model.Workflow{ID: "wf-test", Name: "code test"}
	rc.HeavyWorkGate = make(chan struct{}, 1)
	return rc
}

func runCodeNode(t *testing.T, e CodeExecutor, src string) (model.NodeOutput, error) {
	t.Helper()
	node := &model.Node{Name: "Code", Parameters: map[string]any{"jsCode": src, "mode": "runOnceForEachItem"}}
	return e.Execute(context.Background(), newCodeTestRunContext(), node, model.NodeOutput{{{JSON: map[string]any{}}}})
}

func allowLoopback(t *testing.T) {
	t.Helper()
	prev := allowLoopbackForTests
	allowLoopbackForTests = true
	t.Cleanup(func() { allowLoopbackForTests = prev })
}

func firstJSON(t *testing.T, out model.NodeOutput) map[string]any {
	t.Helper()
	if len(out) != 1 || len(out[0]) != 1 {
		t.Fatalf("expected exactly one output item, got %#v", out)
	}
	return out[0][0].JSON
}

// A script that has no top-level await must keep running exactly as before.
func TestCodeNode_SyncScriptUnchanged(t *testing.T) {
	out, err := runCodeNode(t, CodeExecutor{}, `return { json: { n: 1 + 1 } };`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := firstJSON(t, out)["n"]; got != int64(2) && got != float64(2) {
		t.Fatalf("n = %#v, want 2", got)
	}
}

// Top-level await (valid in n8n Code nodes) must work, including on a
// non-promise value and on an async function's result.
func TestCodeNode_TopLevelAwait(t *testing.T) {
	src := `
async function double(x) { return x * 2; }
const v = await double(21);
const w = await 5;
return { json: { v, w } };`
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	j := firstJSON(t, out)
	if j["v"] != int64(42) || j["w"] != int64(5) {
		t.Fatalf("got %#v, want v=42 w=5", j)
	}
}

// A rejected top-level await must fail the node loudly rather than be
// swallowed (or, worse, exported as an unresolved promise).
func TestCodeNode_TopLevelAwaitRejectionSurfaces(t *testing.T) {
	src := `
async function boom() { throw new Error('nope'); }
await boom();
return { json: {} };`
	_, err := runCodeNode(t, CodeExecutor{}, src)
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("expected the rejection to surface as an error, got %v", err)
	}
}

func TestCodeNode_SyntaxErrorStillReported(t *testing.T) {
	_, err := runCodeNode(t, CodeExecutor{}, `return {{;`)
	if err == nil {
		t.Fatal("expected a syntax error")
	}
}

func TestCodeNode_HelpersHTTPRequest_ObjectBody(t *testing.T) {
	allowLoopback(t)
	var gotAuth, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotUA = r.Header.Get("Authorization"), r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"a:free"},{"id":"b"}]}`))
	}))
	defer srv.Close()

	src := `
const helpers = this.helpers;
const r = await helpers.httpRequest({ method: 'GET', url: '` + srv.URL + `/models', headers: { Authorization: 'Bearer k' }, timeout: 5000, json: true });
return { json: { isArr: Array.isArray(r.data), ids: r.data.filter(m => /:free$/.test(m.id)).map(m => m.id) } };`
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	j := firstJSON(t, out)
	if j["isArr"] != true {
		t.Fatalf("r.data should be a real array, got %#v", j)
	}
	if gotAuth != "Bearer k" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer k")
	}
	if gotUA == "" || strings.HasPrefix(gotUA, "Go-http-client") {
		t.Errorf("expected the shared default User-Agent, got %q", gotUA)
	}
}

// Together's /v1/models returns a bare JSON array; Model Controller checks
// Array.isArray(result), so the helper must return the array itself.
func TestCodeNode_HelpersHTTPRequest_BareArrayBody(t *testing.T) {
	allowLoopback(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"x"},{"id":"y"}]`))
	}))
	defer srv.Close()

	src := `
const r = await this.helpers.httpRequest({ url: '` + srv.URL + `', json: true });
return { json: { isArr: Array.isArray(r), n: r.length } };`
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	j := firstJSON(t, out)
	if j["isArr"] != true || j["n"] != int64(2) {
		t.Fatalf("got %#v, want a 2-element array", j)
	}
}

func TestCodeNode_HelpersHTTPRequest_Non2xxThrowsWithoutLeaking(t *testing.T) {
	allowLoopback(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"secret-body-value"}`))
	}))
	defer srv.Close()

	src := `
try {
  await this.helpers.httpRequest({ url: '` + srv.URL + `/x?key=SECRETKEY', headers: { Authorization: 'Bearer SECRETTOKEN' }, json: true });
  return { json: { threw: false } };
} catch (e) {
  return { json: { threw: true, msg: String(e && e.message || e) } };
}`
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	j := firstJSON(t, out)
	if j["threw"] != true {
		t.Fatalf("expected a non-2xx to throw, got %#v", j)
	}
	msg, _ := j["msg"].(string)
	for _, leak := range []string{"SECRETKEY", "SECRETTOKEN", "secret-body-value", srv.URL} {
		if strings.Contains(msg, leak) {
			t.Errorf("thrown message leaks %q: %q", leak, msg)
		}
	}
	if !strings.Contains(msg, "401") {
		t.Errorf("message should carry the status code, got %q", msg)
	}
}

// The httpRequest node's SSRF boundary must apply to scripts too.
func TestCodeNode_HelpersHTTPRequest_BlocksLoopback(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer srv.Close()

	src := `
try {
  await this.helpers.httpRequest({ url: '` + srv.URL + `', json: true });
  return { json: { threw: false } };
} catch (e) { return { json: { threw: true } }; }`
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if firstJSON(t, out)["threw"] != true || hit {
		t.Fatalf("loopback request must be blocked before reaching the server (hit=%v)", hit)
	}
}

func TestCodeNode_HelpersHTTPRequest_TimeoutHonored(t *testing.T) {
	allowLoopback(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	src := `
try {
  await this.helpers.httpRequest({ url: '` + srv.URL + `', timeout: 200, json: true });
  return { json: { threw: false } };
} catch (e) { return { json: { threw: true } }; }`
	start := time.Now()
	out, err := runCodeNode(t, CodeExecutor{}, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if firstJSON(t, out)["threw"] != true {
		t.Fatal("expected the slow request to be aborted")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("timeout not honored, took %v", d)
	}
}
