package nodes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
)

// allowLoopbackInTest points guardSSRF at "this is a test hitting
// httptest.Server on loopback" for the duration of one test, then
// restores the real (production) SSRF guard -- see allowLoopbackForTests
// in http.go, which defaults false and is never set outside this
// package's own tests.
func allowLoopbackInTest(t *testing.T) {
	t.Helper()
	allowLoopbackForTests = true
	t.Cleanup(func() { allowLoopbackForTests = false })
}

func testRunContext() *engine.RunContext {
	rc := &engine.RunContext{
		Workflow:      &model.Workflow{ID: "wf-test"},
		Execution:     &model.Execution{ID: "exec-test"},
		HeavyWorkGate: make(chan struct{}, 2),
	}
	return rc
}

// TestHTTPRequestNoRetryOnFailNeverErrorsOnStatus is the critical
// backward-compatibility guarantee: an httpRequest node that does NOT
// opt in via RetryOnFail (the existing AI-request-loop nodes -- Unified
// AI Request, Fact AI Request, Semantic AI Request, QC AI Request --
// none of which set RetryOnFail today) must keep succeeding on every
// status code exactly as before, with the JSON body's fields merged
// directly onto the output item (matching n8n's actual default
// behavior -- no options.response.response.fullResponse set), so their
// own body-inspection retry-loop design (Validate Attempt et al., which
// reads $json.error.message / $json.candidates directly, not
// $json.json.error) is completely unaffected by this change.
func TestHTTPRequestNoRetryOnFailNeverErrorsOnStatus(t *testing.T) {
	allowLoopbackInTest(t)
	for _, status := range []int{200, 400, 401, 403, 429, 500, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"denied"}}`))
		}))

		node := &model.Node{Name: "Unified AI Request", Parameters: map[string]any{"url": srv.URL, "method": "GET"}}
		exec := &HTTPRequestExecutor{Client: srv.Client()}
		out, err := exec.Execute(context.Background(), testRunContext(), node, model.NodeOutput{{{JSON: map[string]any{}}}})
		srv.Close()

		if err != nil {
			t.Fatalf("status %d: unexpected error with RetryOnFail unset: %v", status, err)
		}
		if len(out) == 0 || len(out[0]) == 0 {
			t.Fatalf("status %d: expected an output item", status)
		}
		errObj, ok := out[0][0].JSON["error"].(map[string]any)
		if !ok || errObj["message"] != "denied" {
			t.Errorf("status %d: expected $json.error.message == \"denied\" (n8n's default unwrapped shape), got JSON = %#v", status, out[0][0].JSON)
		}
		if _, present := out[0][0].JSON["statusCode"]; present {
			t.Errorf("status %d: statusCode should not be present unless the node requests Full Response", status)
		}
	}
}

// TestHTTPRequestRetryOnFailClassifiesStatus verifies the opt-in
// behavior: a node WITH RetryOnFail set gets real Go errors for
// 401/403 (permanent/configuration) and 429/5xx (transient/retryable)
// so MicroFlow's own retry engine can act on them -- this is what makes
// retryOnFail/maxTries meaningful on an httpRequest node like a
// "Fetch YouTube Trending" or "Fetch Obscure Wiki Topics" call instead
// of silently doing nothing.
func TestHTTPRequestRetryOnFailClassifiesStatus(t *testing.T) {
	allowLoopbackInTest(t)
	cases := []struct {
		status        int
		wantErr       bool
		wantPermanent bool
	}{
		{200, false, false},
		{401, true, true},
		{403, true, true},
		{429, true, false},
		{500, true, false},
		{503, true, false},
	}

	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`denied`))
		}))

		node := &model.Node{
			Name:        "Fetch YouTube Trending",
			Parameters:  map[string]any{"url": srv.URL, "method": "GET"},
			RetryOnFail: true,
			MaxTries:    3,
		}
		exec := &HTTPRequestExecutor{Client: srv.Client()}
		_, err := exec.Execute(context.Background(), testRunContext(), node, model.NodeOutput{{{JSON: map[string]any{}}}})
		srv.Close()

		if tc.wantErr && err == nil {
			t.Errorf("status %d: expected an error with RetryOnFail set", tc.status)
			continue
		}
		if !tc.wantErr && err != nil {
			t.Errorf("status %d: unexpected error: %v", tc.status, err)
			continue
		}
		if tc.wantErr {
			if got := engine.IsPermanent(err); got != tc.wantPermanent {
				t.Errorf("status %d: IsPermanent = %v, want %v (err: %v)", tc.status, got, tc.wantPermanent, err)
			}
		}
	}
}

// TestHTTPRequestDefaultUserAgent verifies every outbound call carries
// a descriptive default User-Agent (Wikimedia's API/robot policy
// requires this and 403s a generic Go client UA), and that a node
// explicitly setting its own User-Agent header is not overridden.
func TestHTTPRequestDefaultUserAgent(t *testing.T) {
	allowLoopbackInTest(t)
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	node := &model.Node{Name: "Fetch Obscure Wiki Topics", Parameters: map[string]any{"url": srv.URL, "method": "GET"}}
	exec := &HTTPRequestExecutor{Client: srv.Client()}
	if _, err := exec.Execute(context.Background(), testRunContext(), node, model.NodeOutput{{{JSON: map[string]any{}}}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotUA == "" || gotUA == "Go-http-client/1.1" {
		t.Errorf("User-Agent = %q, want a descriptive default", gotUA)
	}

	// A node-provided User-Agent header must win over the default.
	os.Unsetenv("MICROFLOW_USER_AGENT")
	node2 := &model.Node{
		Name: "Custom UA Node",
		Parameters: map[string]any{
			"url": srv.URL, "method": "GET",
			"headers": map[string]any{"User-Agent": "MyCustomBot/2.0"},
		},
	}
	if _, err := exec.Execute(context.Background(), testRunContext(), node2, model.NodeOutput{{{JSON: map[string]any{}}}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotUA != "MyCustomBot/2.0" {
		t.Errorf("node-provided User-Agent was overridden: got %q", gotUA)
	}
}

// TestHTTPRequestHeaderExpressionsAreResolved is the regression test for
// the "Unified AI Request" OpenRouter bug: header VALUES must go through
// the same {{ ... }} expression evaluator as url/body. Previously this
// branch set the raw, un-evaluated template string as the header value
// (e.g. the literal text "{{ $json.authValue }}" instead of the resolved
// "Bearer sk-or-...") because it skipped expr.Eval entirely. That made
// every expression-based header -- most critically Authorization --
// arrive at the provider broken, producing an immediate, universal
// 401/403 regardless of whether the underlying API key was ever valid.
func TestHTTPRequestHeaderExpressionsAreResolved(t *testing.T) {
	allowLoopbackInTest(t)
	var gotAuth, gotReferer, gotStaticCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotReferer = r.Header.Get("HTTP-Referer")
		gotStaticCT = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	node := &model.Node{
		Name: "Unified AI Request",
		Parameters: map[string]any{
			"url":    srv.URL,
			"method": "POST",
			"body":   `{{ $json.bodyStr }}`,
			"headers": map[string]any{
				"Authorization": "{{ $json.authValue }}",
				"Content-Type":  "application/json",
				"HTTP-Referer":  "{{ $json.refererValue }}",
			},
		},
	}
	exec := &HTTPRequestExecutor{Client: srv.Client()}
	input := model.NodeOutput{{{JSON: map[string]any{
		"bodyStr":      `{"model":"test"}`,
		"authValue":    "Bearer sk-or-real-key-123",
		"refererValue": "https://example.com",
	}}}}
	if _, err := exec.Execute(context.Background(), testRunContext(), node, input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAuth != "Bearer sk-or-real-key-123" {
		t.Errorf("Authorization header not resolved: got %q, want %q (bug: raw template string sent as-is)", gotAuth, "Bearer sk-or-real-key-123")
	}
	if gotReferer != "https://example.com" {
		t.Errorf("HTTP-Referer header not resolved: got %q, want %q", gotReferer, "https://example.com")
	}
	if gotStaticCT != "application/json" {
		t.Errorf("static (non-expression) header value broken by fix: got %q", gotStaticCT)
	}
}
