//go:build unix

package localmodel

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain doubles as the fake llama-server: when FAKE_LLAMA=1 the test binary
// serves /health and /v1/chat/completions like the real thing.
func TestMain(m *testing.M) {
	if os.Getenv("FAKE_LLAMA") == "1" {
		fakeServer()
		return
	}
	os.Exit(m.Run())
}

func fakeServer() {
	args := os.Args[1:]
	for _, a := range args {
		if a == "--version" {
			fmt.Println("version: fake-1 (build 1)")
			return
		}
	}
	var port, key string
	for i, a := range args {
		switch a {
		case "--port":
			port = args[i+1]
		case "--api-key":
			key = args[i+1]
		}
	}
	_ = flag.CommandLine
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, "unauthorized", 401)
			return
		}
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		content := "<think>hmm</think>hello from fake"
		if msgs, _ := req["messages"].([]any); len(msgs) > 0 {
			if m0, _ := msgs[0].(map[string]any); m0 != nil && m0["content"] == "fail" {
				http.Error(w, "boom", 500)
				return
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}})
	})
	http.ListenAndServe("127.0.0.1:"+port, mux)
}

func tarGz(t *testing.T, files map[string]string, extra func(*tar.Writer)) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg})
		tw.Write([]byte(body))
	}
	if extra != nil {
		extra(tw)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func sha(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func newTestManager(t *testing.T) (*Manager, *httptest.Server) {
	exe, _ := os.Executable()
	t.Setenv("FAKE_LLAMA", "1")
	t.Setenv("FAKE_BIN", exe)
	t.Setenv("RENDER", "")
	script := "#!/bin/sh\nexec \"$FAKE_BIN\" \"$@\"\n"
	rt := tarGz(t, map[string]string{"llama-x/llama-server": script}, nil)
	model := append([]byte("GGUF"), bytes.Repeat([]byte{7}, 4096)...)
	mux := http.NewServeMux()
	mux.HandleFunc("/rt.tar.gz", func(w http.ResponseWriter, r *http.Request) { w.Write(rt) })
	mux.HandleFunc("/model.gguf", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "m", time.Time{}, bytes.NewReader(model))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	m := New(t.TempDir(), func(string, ...any) {})
	m.blocked = ""
	m.sp = spec{
		RuntimeURL: srv.URL + "/rt.tar.gz", RuntimeName: "rt.tar.gz", RuntimeSHA256: sha(rt),
		ModelURLs: []string{srv.URL + "/model.gguf"}, ModelSHA256: sha(model),
		AllowAnyHost: true, RuntimeMinBytes: 10, MinModelBytes: 1000, MaxModelBytes: 1 << 20,
	}
	return m, srv
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestFullLifecycle(t *testing.T) {
	m, _ := newTestManager(t)
	if err := m.Install(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "install", func() bool { s := m.Status(); return !s.Installing })
	s := m.Status()
	if !s.Installed || s.InstallError != "" || s.RuntimeVersion == "" {
		t.Fatalf("install failed: %+v", s)
	}
	if err := m.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	if m.Available() {
		t.Fatal("must not be available before start")
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "ready", func() bool { return m.Available() })
	resp, err := m.Chat(context.Background(), map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	txt, _ := ExtractText(resp)
	if txt != "hello from fake" {
		t.Fatalf("got %q", txt)
	}
	pid := m.Status().PID
	if err := m.Stop(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "stopped", func() bool { return m.Status().Process == "stopped" })
	if m.Available() {
		t.Fatal("available after stop")
	}
	if processLooksLike(pid, "llama") {
		t.Fatal("process still alive after Stop")
	}
	if err := m.Uninstall(); err != nil {
		t.Fatal(err)
	}
	s = m.Status()
	if s.Installed || s.Enabled {
		t.Fatalf("uninstall left state: %+v", s)
	}
	for _, sub := range []string{"runtime", "models"} {
		if _, err := os.Stat(filepath.Join(m.dir, sub)); err == nil {
			t.Fatalf("%s not removed", sub)
		}
	}
}

func TestBadHashAbortsAndCleansUp(t *testing.T) {
	m, _ := newTestManager(t)
	m.sp.ModelSHA256 = strings.Repeat("0", 64)
	if err := m.Install(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "install end", func() bool { return !m.Status().Installing })
	s := m.Status()
	if s.Installed || !strings.Contains(s.InstallError, "SHA-256") {
		t.Fatalf("expected hash failure, got %+v", s)
	}
	if _, err := os.Stat(m.modelPath() + ".part"); err == nil {
		t.Fatal("bad partial file kept")
	}
	if _, err := os.Stat(m.modelPath()); err == nil {
		t.Fatal("bad model kept")
	}
}

func TestCircuitBreaker(t *testing.T) {
	m, _ := newTestManager(t)
	m.Install()
	waitFor(t, "install", func() bool { return !m.Status().Installing })
	m.SetEnabled(true)
	m.Start()
	defer m.Shutdown()
	waitFor(t, "ready", m.Available)
	for i := 0; i < 3; i++ {
		if _, err := m.Chat(context.Background(), map[string]any{"messages": []any{map[string]any{"role": "user", "content": "fail"}}}); err == nil {
			t.Fatal("expected failure")
		}
	}
	if m.Available() {
		t.Fatal("breaker should have opened")
	}
	if m.Status().CooldownSeconds == 0 {
		t.Fatal("cooldown not reported")
	}
}

func TestBlockedOnRender(t *testing.T) {
	t.Setenv("RENDER", "true")
	m := New(t.TempDir(), func(string, ...any) {})
	if m.Install() == nil || m.Start() == nil {
		t.Fatal("install/start must be refused on Render")
	}
	if m.Status().Supported {
		t.Fatal("must report unsupported")
	}
}

func TestExtractRejectsTraversalAndEscapingSymlink(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.gz")
	os.WriteFile(bad, tarGz(t, map[string]string{"../evil": "x"}, nil), 0o644)
	if err := extractArchive(bad, filepath.Join(dir, "o1")); err == nil {
		t.Fatal("path traversal accepted")
	}
	lnk := filepath.Join(dir, "lnk.tar.gz")
	os.WriteFile(lnk, tarGz(t, nil, func(tw *tar.Writer) {
		tw.WriteHeader(&tar.Header{Name: "a/l", Typeflag: tar.TypeSymlink, Linkname: "../../../etc/passwd"})
	}), 0o644)
	if err := extractArchive(lnk, filepath.Join(dir, "o2")); err == nil {
		t.Fatal("escaping symlink accepted")
	}
}

func TestStripThink(t *testing.T) {
	for in, want := range map[string]string{
		"<think>a</think>ok": "ok", "ok": "ok", "<think>unterminated": "", "x</think> y": "y",
	} {
		if got := StripThink(in); got != want {
			t.Errorf("%q -> %q want %q", in, got, want)
		}
	}
}
