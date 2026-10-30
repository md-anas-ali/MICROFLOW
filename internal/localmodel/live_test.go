//go:build unix

package localmodel

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Opt-in (needs internet): downloads the REAL pinned llama.cpp runtime for this
// platform, checks its SHA-256, unpacks it and runs `llama-server --version`.
// MICROFLOW_LIVE_TEST=1 go test ./internal/localmodel -run Live -v
func TestLiveRuntimeInstall(t *testing.T) {
	if os.Getenv("MICROFLOW_LIVE_TEST") != "1" {
		t.Skip("set MICROFLOW_LIVE_TEST=1 to download the real runtime")
	}
	m := New(t.TempDir(), t.Logf)
	if m.blocked != "" {
		t.Skip(m.blocked)
	}
	archive := filepath.Join(m.dir, "downloads", m.sp.RuntimeName)
	if err := m.download(context.Background(), []string{m.sp.RuntimeURL}, archive, m.sp.RuntimeSHA256, 1<<20, 512<<20, false, nil); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(m.dir, "runtime")
	if err := extractArchive(archive, root); err != nil {
		t.Fatal(err)
	}
	bin, err := findServerBinary(root)
	if err != nil {
		t.Fatal(err)
	}
	ver, err := verifyRuntime(bin)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("runtime ok: %s", ver)
}
