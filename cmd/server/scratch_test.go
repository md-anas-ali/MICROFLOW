package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveScratchRoot(t *testing.T) {
	base := t.TempDir()

	// 1) writable preferred dir is used as-is, and no probe file is left behind.
	pref := filepath.Join(base, "scratch")
	if got := resolveScratchRoot(pref); got != pref {
		t.Fatalf("writable dir: got %q want %q", got, pref)
	}
	if e, _ := os.ReadDir(pref); len(e) != 0 {
		t.Fatalf("probe file left behind: %d entries", len(e))
	}

	// A regular file used as a parent makes every MkdirAll below it fail,
	// which simulates "Storage missing" without needing special privileges.
	blocker := filepath.Join(base, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(blocker, "microflow")

	// 2) preferred unusable but OS temp dir fine -> falls back to it.
	tmp := filepath.Join(base, "tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", tmp)
	if got, want := resolveScratchRoot(bad), filepath.Join(tmp, "microflow"); got != want {
		t.Fatalf("fallback to temp: got %q want %q", got, want)
	}

	// 3) nothing usable at all -> no panic, configured path kept, server can still start.
	t.Setenv("TMPDIR", filepath.Join(blocker, "tmp"))
	if got := resolveScratchRoot(bad); got != bad {
		t.Fatalf("no storage: got %q want %q", got, bad)
	}
}
