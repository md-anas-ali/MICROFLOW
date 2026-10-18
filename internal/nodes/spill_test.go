package nodes

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"microflow/internal/engine"
	"microflow/internal/model"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func dirEntries(t *testing.T, dir string) int {
	t.Helper()
	e, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	return len(e)
}

func TestReadBodyCappedSmallStaysInRAM(t *testing.T) {
	dir := t.TempDir()
	got, err := readBodyCapped(strings.NewReader("hello"), 1<<20, dir)
	if err != nil || string(got) != "hello" {
		t.Fatalf("got %q err %v", got, err)
	}
	if n := dirEntries(t, dir); n != 0 {
		t.Fatalf("small body touched disk: %d entries", n)
	}
}

func TestReadBodyCappedLargeContentAndCleanup(t *testing.T) {
	t.Setenv("MICROFLOW_SPILL_THRESHOLD_KB", "64")
	dir := t.TempDir()
	want := bytes.Repeat([]byte("abcdefghij"), 50000) // 500 KB
	got, err := readBodyCapped(bytes.NewReader(want), 4<<20, dir)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("content mismatch err=%v len=%d", err, len(got))
	}
	if n := dirEntries(t, dir); n != 0 {
		t.Fatalf("temp file not cleaned up: %d entries", n)
	}
}

func TestReadBodyCappedOverCapBothPaths(t *testing.T) {
	t.Setenv("MICROFLOW_SPILL_THRESHOLD_KB", "64")
	big := bytes.Repeat([]byte("x"), 300000)
	dir := t.TempDir()
	if _, err := readBodyCapped(bytes.NewReader(big), 200000, dir); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("storage path: want errBodyTooLarge, got %v", err)
	}
	if n := dirEntries(t, dir); n != 0 {
		t.Fatalf("over-cap temp file not removed: %d", n)
	}
	if _, err := readBodyCapped(bytes.NewReader(big), 200000, ""); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("RAM path: want errBodyTooLarge, got %v", err)
	}
	// exactly at the cap is allowed on both paths
	ok := bytes.Repeat([]byte("y"), 200000)
	for _, d := range []string{dir, ""} {
		got, err := readBodyCapped(bytes.NewReader(ok), 200000, d)
		if err != nil || len(got) != 200000 {
			t.Fatalf("at-cap dir=%q: len=%d err=%v", d, len(got), err)
		}
	}
}

func TestReadBodyCappedNoStorageFallsBack(t *testing.T) {
	t.Setenv("MICROFLOW_SPILL_THRESHOLD_KB", "64")
	want := bytes.Repeat([]byte("z"), 400000)
	// (a) empty dir, (b) unwritable path (a file where a dir is needed), (c) feature off
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"", filepath.Join(file, "sub")} {
		got, err := readBodyCapped(bytes.NewReader(want), 4<<20, d)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("dir=%q fallback failed: err=%v len=%d", d, err, len(got))
		}
	}
	t.Setenv("MICROFLOW_STORAGE_SPILL", "0")
	dir := t.TempDir()
	got, err := readBodyCapped(bytes.NewReader(want), 4<<20, dir)
	if err != nil || !bytes.Equal(got, want) || dirEntries(t, dir) != 0 {
		t.Fatalf("spill=0 must stay RAM-only: err=%v entries=%d", err, dirEntries(t, dir))
	}
}

func TestReadBodyCappedReadError(t *testing.T) {
	t.Setenv("MICROFLOW_SPILL_THRESHOLD_KB", "1")
	boom := errors.New("boom")
	r := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("a"), 5000)), &errReader{boom})
	dir := t.TempDir()
	if _, err := readBodyCapped(r, 1<<20, dir); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if n := dirEntries(t, dir); n != 0 {
		t.Fatalf("temp file left after read error: %d", n)
	}
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

func TestFileToBase64AndStringMatchOldBehaviour(t *testing.T) {
	dir := t.TempDir()
	for _, size := range []int{0, 1, 2, 3, 4, 100, 65536, 65537, 300001} {
		raw := make([]byte, size)
		for i := range raw {
			raw[i] = byte(i*7 + 3)
		}
		p := filepath.Join(dir, "f.bin")
		if err := os.WriteFile(p, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := fileToBase64(p, int64(size))
		if err != nil || got != base64.StdEncoding.EncodeToString(raw) {
			t.Fatalf("size %d base64 mismatch err=%v", size, err)
		}
		s, err := fileToString(p, int64(size))
		if err != nil || s != string(raw) {
			t.Fatalf("size %d string mismatch err=%v", size, err)
		}
	}
	if _, err := fileToBase64(filepath.Join(dir, "missing"), 1); err == nil {
		t.Fatal("missing file must error, not panic")
	}
}

// Measures peak extra heap while reading a 24 MB body: old io.ReadAll path
// vs readBodyCapped. Logged, with a soft assertion that the new path is not worse.
func TestPeakHeapOldVsNew24MB(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	body := bytes.Repeat([]byte("0123456789abcdef"), 24*1024*1024/16)
	measure := func(f func()) uint64 {
		runtime.GC()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		f()
		runtime.ReadMemStats(&b)
		return b.TotalAlloc - a.TotalAlloc
	}
	oldAlloc := measure(func() {
		b, _ := io.ReadAll(io.LimitReader(bytes.NewReader(body), maxResponseBytes+1))
		_ = b
	})
	dir := t.TempDir()
	newAlloc := measure(func() {
		b, _ := readBodyCapped(bytes.NewReader(body), maxResponseBytes, dir)
		_ = b
	})
	t.Logf("24MB body: old ReadAll allocated %.1f MB, new readBodyCapped allocated %.1f MB", float64(oldAlloc)/1e6, float64(newAlloc)/1e6)
	if newAlloc > oldAlloc {
		t.Fatalf("new path allocates more than old (%d > %d)", newAlloc, oldAlloc)
	}
}

// End-to-end through the real HTTP node: a ~3 MB JSON body must decode to the
// same result with Storage usable, with Storage missing, and with spill off,
// and must leave nothing behind in the scratch dir.
func TestHTTPNodeLargeJSONSameResultWithAndWithoutStorage(t *testing.T) {
	old := allowLoopbackForTests
	allowLoopbackForTests = true
	t.Cleanup(func() { allowLoopbackForTests = old })

	payload := strings.Repeat("A", 3<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"image":"` + payload + `"}`))
	}))
	defer srv.Close()

	run := func(t *testing.T, scratch string) map[string]any {
		t.Helper()
		e := &HTTPRequestExecutor{Client: srv.Client()}
		rc := &engine.RunContext{Execution: &model.Execution{}, ScratchDir: scratch, HeavyWorkGate: make(chan struct{}, 1)}
		node := &model.Node{Name: "T", Parameters: map[string]any{"method": "GET", "url": srv.URL}}
		out, err := e.Execute(context.Background(), rc, node, model.NodeOutput{{model.Item{JSON: map[string]any{}}}})
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		return out[0][0].JSON
	}
	check := func(t *testing.T, j map[string]any) {
		t.Helper()
		if j["ok"] != true || j["image"] != payload {
			t.Fatalf("decoded result differs (ok=%v len=%d)", j["ok"], len(fmt.Sprint(j["image"])))
		}
	}

	t.Run("storage usable", func(t *testing.T) {
		dir := t.TempDir()
		check(t, run(t, dir))
		if n := dirEntries(t, dir); n != 0 {
			t.Fatalf("temp file left in scratch: %d", n)
		}
	})
	t.Run("storage missing (empty ScratchDir)", func(t *testing.T) { check(t, run(t, "")) })
	t.Run("storage broken (unwritable path)", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "file")
		_ = os.WriteFile(f, []byte("x"), 0o600)
		check(t, run(t, filepath.Join(f, "sub")))
	})
	t.Run("spill disabled", func(t *testing.T) {
		t.Setenv("MICROFLOW_STORAGE_SPILL", "0")
		dir := t.TempDir()
		check(t, run(t, dir))
		if n := dirEntries(t, dir); n != 0 {
			t.Fatalf("spill disabled but disk touched: %d", n)
		}
	})
}
