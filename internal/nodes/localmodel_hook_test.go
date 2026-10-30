package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
)

type fakeLocal struct {
	avail  bool
	finish string
	err    error
	calls  int32
	last   map[string]any
}

func (f *fakeLocal) Available() bool { return f.avail }
func (f *fakeLocal) MaxContext() int { return 4096 }
func (f *fakeLocal) Chat(_ context.Context, req map[string]any) (map[string]any, error) {
	atomic.AddInt32(&f.calls, 1)
	f.last = req
	if f.err != nil {
		return nil, f.err
	}
	fin := f.finish
	if fin == "" {
		fin = "stop"
	}
	return map[string]any{"choices": []any{map[string]any{"finish_reason": fin, "message": map[string]any{"role": "assistant", "content": "LOCAL ANSWER"}}}}, nil
}

func runHTTP(t *testing.T, e *HTTPRequestExecutor, srvURL, body string, opts map[string]any) map[string]any {
	t.Helper()
	old := allowLoopbackForTests
	allowLoopbackForTests = true
	defer func() { allowLoopbackForTests = old }()
	rc := &engine.RunContext{Execution: &model.Execution{ID: "exec-1"}, ScratchDir: t.TempDir(), HeavyWorkGate: make(chan struct{}, 1)}
	params := map[string]any{"method": "POST", "url": srvURL + "/api/v1/chat/completions", "body": body}
	if opts != nil {
		params["options"] = opts
	}
	node := &model.Node{Name: "Unified AI Request", Parameters: params}
	out, err := e.Execute(context.Background(), rc, node, model.NodeOutput{{model.Item{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	return out[0][0].JSON
}

func provider(t *testing.T) (*httptest.Server, *int32) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"REMOTE ANSWER"}}]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func content(j map[string]any) string {
	return j["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"].(string)
}

const body1 = `{"model":"x/free:free","messages":[{"role":"user","content":"hello"}],"temperature":0.2,"max_tokens":300}`

func TestLocalFirstThenFallbacks(t *testing.T) {
	srv, hits := provider(t)

	t.Run("nil local = unchanged behaviour", func(t *testing.T) {
		e := &HTTPRequestExecutor{Client: srv.Client()}
		if got := content(runHTTP(t, e, srv.URL, body1, nil)); got != "REMOTE ANSWER" {
			t.Fatal(got)
		}
	})
	t.Run("disabled/unavailable -> provider", func(t *testing.T) {
		f := &fakeLocal{avail: false}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		if got := content(runHTTP(t, e, srv.URL, body1, nil)); got != "REMOTE ANSWER" || f.calls != 0 {
			t.Fatalf("got %q calls=%d", got, f.calls)
		}
	})
	t.Run("available -> local, provider untouched", func(t *testing.T) {
		before := atomic.LoadInt32(hits)
		f := &fakeLocal{avail: true}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		j := runHTTP(t, e, srv.URL, body1, nil)
		if content(j) != "LOCAL ANSWER" || atomic.LoadInt32(hits) != before {
			t.Fatalf("local not used: %v", j)
		}
		if f.last["temperature"] != 0.2 {
			t.Fatalf("request params not carried over: %v", f.last)
		}
	})
	t.Run("retry of same prompt goes to provider", func(t *testing.T) {
		f := &fakeLocal{avail: true}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		first := content(runHTTP(t, e, srv.URL, body1, nil))
		retry := content(runHTTP(t, e, srv.URL, strings.Replace(body1, "x/free:free", "y/other:free", 1), nil))
		if first != "LOCAL ANSWER" || retry != "REMOTE ANSWER" {
			t.Fatalf("first=%q retry=%q", first, retry)
		}
	})
	t.Run("local error -> provider", func(t *testing.T) {
		f := &fakeLocal{avail: true, err: errors.New("boom")}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		if got := content(runHTTP(t, e, srv.URL, body1, nil)); got != "REMOTE ANSWER" {
			t.Fatal(got)
		}
	})
	t.Run("truncated local answer -> provider", func(t *testing.T) {
		f := &fakeLocal{avail: true, finish: "length"}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		if got := content(runHTTP(t, e, srv.URL, body1, nil)); got != "REMOTE ANSWER" {
			t.Fatal(got)
		}
	})
	t.Run("image part -> provider", func(t *testing.T) {
		f := &fakeLocal{avail: true}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		b := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:x"}}]}]}`
		if got := content(runHTTP(t, e, srv.URL, b, nil)); got != "REMOTE ANSWER" || f.calls != 0 {
			t.Fatal(got)
		}
	})
	t.Run("tools -> provider", func(t *testing.T) {
		f := &fakeLocal{avail: true}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		b := `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"function"}]}`
		if got := content(runHTTP(t, e, srv.URL, b, nil)); got != "REMOTE ANSWER" || f.calls != 0 {
			t.Fatal(got)
		}
	})
	t.Run("fullResponse shape", func(t *testing.T) {
		f := &fakeLocal{avail: true}
		e := &HTTPRequestExecutor{Client: srv.Client(), Local: f}
		j := runHTTP(t, e, srv.URL, body1, map[string]any{"fullResponse": true})
		b, _ := j["body"].(map[string]any)
		if j["statusCode"] != 200 || b == nil || content(b) != "LOCAL ANSWER" {
			t.Fatalf("%v", j)
		}
	})
}

func TestGeminiTranslation(t *testing.T) {
	f := &fakeLocal{avail: true}
	e := &HTTPRequestExecutor{Local: f}
	body := `{"systemInstruction":{"parts":[{"text":"be brief"}]},"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"temperature":0.3,"maxOutputTokens":500,"responseMimeType":"application/json"}}`
	node := &model.Node{Name: "n", Parameters: map[string]any{}}
	rc := &engine.RunContext{Execution: &model.Execution{ID: "g"}}
	res, ok := e.tryLocal(context.Background(), rc, node, "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent", body)
	if !ok {
		t.Fatal("gemini request not served locally")
	}
	txt := res["candidates"].([]any)[0].(map[string]any)["content"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"]
	if txt != "LOCAL ANSWER" {
		t.Fatalf("%v", res)
	}
	msgs := f.last["messages"].([]any)
	if len(msgs) != 2 || msgs[0].(map[string]any)["role"] != "system" || f.last["response_format"] == nil {
		b, _ := json.Marshal(f.last)
		t.Fatalf("bad translation: %s", b)
	}
	// non-text parts and schemas must NOT be handled locally
	for _, bad := range []string{
		`{"contents":[{"parts":[{"inlineData":{"mimeType":"image/png","data":"x"}}]}]}`,
		`{"contents":[{"parts":[{"text":"x"}]}],"generationConfig":{"responseSchema":{"type":"OBJECT"}}}`,
	} {
		if _, ok := e.tryLocal(context.Background(), rc, node, "https://generativelanguage.googleapis.com/v1beta/models/m:generateContent", bad); ok {
			t.Fatalf("should fall back for %s", bad)
		}
	}
	// non-AI URL never touches the local model
	calls := f.calls
	if _, ok := e.tryLocal(context.Background(), rc, node, "https://example.com/api", body); ok || f.calls != calls {
		t.Fatal("non-AI request intercepted")
	}
}
