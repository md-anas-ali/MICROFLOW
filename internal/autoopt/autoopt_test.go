package autoopt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *logSink) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func newTestOpt(t *testing.T, roots ...string) (*Optimizer, *logSink) {
	t.Helper()
	ls := &logSink{}
	return New(Config{Enabled: true, PhaseTimeout: 300 * time.Millisecond, TermGrace: 500 * time.Millisecond, TempRoots: roots, Logf: ls.logf}), ls
}

type fnCloser struct{ f func() error }

func (c fnCloser) Close() error { return c.f() }

// Nil optimizer / nil context are valid no-ops and Run is plain cmd.Run().
func TestNilSafe(t *testing.T) {
	var o *Optimizer
	if o.Enabled() {
		t.Fatal("nil optimizer must be disabled")
	}
	sc := o.Begin("x", "y")
	if sc != nil {
		t.Fatal("disabled Begin must return nil")
	}
	sc.TrackCancel("a", func() {})
	sc.TrackStop("a", func() {})
	sc.TrackTemp("/nope")
	sc.TrackCloser("a", io.NopCloser(strings.NewReader("")))
	sc.Release()
	body := io.NopCloser(strings.NewReader("hi"))
	if got := sc.WrapBody(body); got != body {
		t.Fatal("nil WrapBody must return the body unchanged")
	}
	o.PostService(context.Background())
	o.PreServiceCheck(context.Background())
	if _, err := exec.LookPath("true"); err == nil {
		if err := sc.Run(exec.Command("true")); err != nil {
			t.Fatalf("nil-context Run should behave like cmd.Run: %v", err)
		}
	}
}

// Phases run in the documented order and Release is idempotent.
func TestReleaseOrderAndIdempotent(t *testing.T) {
	root := t.TempDir()
	o, _ := newTestOpt(t, root)
	sc := o.Begin("e1", "wf")
	var mu sync.Mutex
	var order []string
	rec := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	tmp := filepath.Join(root, "scratch-e1")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	sc.TrackRelease("mem", func() { rec("memory") })
	sc.TrackTemp(tmp)
	sc.TrackStop("timer", func() { rec("timers") })
	sc.TrackCloser("sock", fnCloser{func() error { rec("sockets"); return nil }})
	sc.TrackCancel("bg", func() { rec("cancel") })
	sc.Release()
	sc.Release() // no-op

	want := []string{"cancel", "sockets", "timers", "memory"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("service-owned temp dir should have been removed")
	}
	if o.LiveContexts() != 0 {
		t.Fatal("context must be forgotten after Release")
	}
}

// A path outside the allowed roots is refused, never deleted.
func TestTempOutsideRootsRefused(t *testing.T) {
	root := t.TempDir()
	other := t.TempDir()
	victim := filepath.Join(other, "keep.txt")
	if err := os.WriteFile(victim, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	o, ls := newTestOpt(t, root)
	sc := o.Begin("e", "wf")
	sc.TrackTemp(victim)
	sc.TrackTemp(root) // the root itself must also be refused
	sc.Release()
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("file outside roots was deleted: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("the temp root itself was deleted: %v", err)
	}
	o.PostService(context.Background())
	if !strings.Contains(ls.text(), "Cleanup warning") {
		t.Fatal("refusal should be logged as a warning")
	}
}

// Test 5: a hanging and a panicking cleanup never stop the remaining phases
// and never hang Release.
func TestCleanupFailureIsolated(t *testing.T) {
	root := t.TempDir()
	o, ls := newTestOpt(t, root)
	sc := o.Begin("e", "wf")
	tmp := filepath.Join(root, "x")
	_ = os.MkdirAll(tmp, 0o700)
	block := make(chan struct{})
	defer close(block)
	sc.TrackStop("hangs", func() { <-block })
	sc.TrackCloser("fails", fnCloser{func() error { return errors.New("boom") }})
	sc.TrackCancel("panics", func() { panic("kaboom") })
	sc.TrackTemp(tmp)

	done := make(chan struct{})
	go func() { sc.Release(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Release hung on a stuck cleanup")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("later phase (temp cleanup) must still run after earlier failures")
	}
	o.PostService(context.Background())
	out := ls.text()
	for _, w := range []string{"panic", "timed out", "boom"} {
		if !strings.Contains(out, w) {
			t.Fatalf("expected warning mentioning %q in:\n%s", w, out)
		}
	}
}

// Test 4: an unclosed HTTP body is found and closed; a normally closed one is not counted.
func TestWrapBodyLeftoverClosed(t *testing.T) {
	o, ls := newTestOpt(t)
	sc := o.Begin("e", "wf")
	var closedA, closedB int32
	a := sc.WrapBody(readCloser{Reader: strings.NewReader("a"), onClose: func() { atomic.AddInt32(&closedA, 1) }})
	_ = sc.WrapBody(readCloser{Reader: strings.NewReader("b"), onClose: func() { atomic.AddInt32(&closedB, 1) }})
	b, _ := io.ReadAll(a)
	if string(b) != "a" {
		t.Fatal("wrapped body must pass reads through unchanged")
	}
	_ = a.Close()
	sc.Release() // closes only the still-open body b
	if atomic.LoadInt32(&closedB) != 1 {
		t.Fatal("unclosed body must be closed by Release")
	}
	if atomic.LoadInt32(&closedA) != 1 {
		t.Fatal("normally closed body must not be closed twice")
	}
	o.PostService(context.Background())
	if !strings.Contains(ls.text(), "unclosed bodies closed: 1") {
		t.Fatalf("log should report exactly one leftover body:\n%s", ls.text())
	}
}

type readCloser struct {
	io.Reader
	onClose func()
}

func (r readCloser) Close() error { r.onClose(); return nil }

// Test 7: Releasing one context never touches another context's resources.
func TestOwnershipIsolation(t *testing.T) {
	o, _ := newTestOpt(t)
	a, b := o.Begin("A", "wfA"), o.Begin("B", "wfB")
	var bClosed int32
	b.TrackCloser("b-sock", fnCloser{func() error { atomic.AddInt32(&bClosed, 1); return nil }})
	a.TrackCloser("a-sock", fnCloser{func() error { return nil }})
	a.Release()
	if atomic.LoadInt32(&bClosed) != 0 {
		t.Fatal("releasing A closed B's resource")
	}
	if o.LiveContexts() != 1 {
		t.Fatalf("B should still be live, live=%d", o.LiveContexts())
	}
	b.Release()
	if atomic.LoadInt32(&bClosed) != 1 {
		t.Fatal("B's own release must close B's resource")
	}
}

// Global steps run in phase order regardless of registration order, a failing
// or panicking step does not stop the rest, and verifier failures only warn.
func TestPostServiceStepsOrderAndIsolation(t *testing.T) {
	o, ls := newTestOpt(t)
	var mu sync.Mutex
	var order []string
	rec := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	o.AddStep(PhaseRuntimeCleanup, "gc", func(context.Context) error { rec("gc"); return nil })
	o.AddStep(PhaseTempResources, "temp", func(context.Context) error { rec("temp"); return errors.New("disk busy") })
	o.AddStep(PhaseReleaseHTTP, "http", func(context.Context) error { rec("http"); panic("oops") })
	o.AddVerifier("v", func() error { return errors.New("not clean") })
	o.PostService(context.Background())
	if strings.Join(order, ",") != "http,temp,gc" {
		t.Fatalf("order = %v", order)
	}
	out := ls.text()
	for _, w := range []string{"Starting cleanup", "disk busy", "panic", "not clean", "Cleanup verification completed with"} {
		if !strings.Contains(out, w) {
			t.Fatalf("missing %q in log:\n%s", w, out)
		}
	}
}

// Pre-service health check recovers a stale context and never blocks.
func TestPreServiceCheckRecoversStale(t *testing.T) {
	o, ls := newTestOpt(t)
	sc := o.Begin("stale", "wf")
	var closed int32
	sc.TrackCloser("s", fnCloser{func() error { atomic.AddInt32(&closed, 1); return nil }})
	o.AddHealthCheck("bad", func() error { return errors.New("inconsistent") })
	o.PreServiceCheck(context.Background())
	if atomic.LoadInt32(&closed) != 1 || o.LiveContexts() != 0 {
		t.Fatal("stale context must be released by the health check")
	}
	out := ls.text()
	if !strings.Contains(out, "inconsistent") || !strings.Contains(out, "Next service allowed") {
		t.Fatalf("health check must warn and still allow the next service:\n%s", out)
	}
}

func TestDisabledIsNoOp(t *testing.T) {
	ls := &logSink{}
	o := New(Config{Enabled: false, Logf: ls.logf})
	if o.Begin("x", "y") != nil {
		t.Fatal("disabled Begin must be nil")
	}
	o.AddStep(PhaseRuntimeCleanup, "never", func(context.Context) error { t.Fatal("step ran while disabled"); return nil })
	o.PostService(context.Background())
	o.PreServiceCheck(context.Background())
	if ls.text() != "" {
		t.Fatalf("disabled optimizer must not log: %q", ls.text())
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("MICROFLOW_AUTO_OPTIMIZE", "off")
	if ConfigFromEnv().Enabled {
		t.Fatal("off must disable")
	}
	t.Setenv("MICROFLOW_AUTO_OPTIMIZE", "")
	t.Setenv("MICROFLOW_AUTO_OPTIMIZE_PHASE_TIMEOUT_SECONDS", "7")
	c := ConfigFromEnv()
	if !c.Enabled || c.PhaseTimeout != 7*time.Second {
		t.Fatalf("bad config: %+v", c)
	}
}
