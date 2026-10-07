package runner

// leftovers.go: between-run cleanup of junk that a finished Service left on
// disk, so the next Service starts from a clean slate.
//
// Two kinds of leftovers are removed -- both strictly temporary files:
//
//  1. Per-execution scratch directories under ScratchRoot that no running
//     execution owns (e.g. left behind by a crash or a failed removal).
//  2. Files/dirs that workflow scripts wrote directly under the OS temp dir
//     (hard-coded paths like /tmp/scene_1.jpg, which escape the scratch dir).
//     Only entries that did NOT exist when the server started are removed.
//
// Nothing in the database (Services, Workflows, Environment, Credentials,
// static data, schedules) is touched. Nothing is removed while any execution
// is still live. MICROFLOW_CLEAN_TMP=0 disables part 2.

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

func (r *Runner) anyLive() bool {
	r.liveExecMu.Lock()
	defer r.liveExecMu.Unlock()
	return len(r.liveExecs) > 0
}

// LiveExecutionCount is how many executions are running right now in this
// process (used by Auto Optimize's pre-service health check).
func (r *Runner) LiveExecutionCount() int {
	r.liveExecMu.Lock()
	defer r.liveExecMu.Unlock()
	return len(r.liveExecs)
}

// InitTempBaseline must be called once at startup, before any run starts. It
// removes scratch directories orphaned by a previous process and records which
// entries already live under the OS temp dir (those are never deleted).
func (r *Runner) InitTempBaseline() {
	if r.ScratchRoot != "" {
		if entries, err := os.ReadDir(r.ScratchRoot); err == nil {
			for _, e := range entries {
				_ = os.RemoveAll(filepath.Join(r.ScratchRoot, e.Name()))
			}
		}
	}
	base := map[string]struct{}{}
	if entries, err := os.ReadDir(os.TempDir()); err == nil {
		for _, e := range entries {
			base[e.Name()] = struct{}{}
		}
	}
	r.tmpMu.Lock()
	r.tmpBaseline = base
	r.tmpMu.Unlock()
}

// CleanupLeftovers removes the junk described above. Safe to call after every
// finished job; it does nothing while an execution is live.
func (r *Runner) CleanupLeftovers() {
	if r.anyLive() {
		return
	}
	removed := 0

	// 1. Orphaned scratch directories (no live execution owns any of them).
	if r.ScratchRoot != "" {
		if entries, err := os.ReadDir(r.ScratchRoot); err == nil {
			for _, e := range entries {
				if err := os.RemoveAll(filepath.Join(r.ScratchRoot, e.Name())); err == nil {
					removed++
				}
			}
		}
	}

	// 2. New entries directly under the OS temp dir.
	if os.Getenv("MICROFLOW_CLEAN_TMP") == "0" {
		return
	}
	r.tmpMu.Lock()
	base := r.tmpBaseline
	r.tmpMu.Unlock()
	if base == nil { // InitTempBaseline never ran: do not guess what is junk
		return
	}
	tmp := filepath.Clean(os.TempDir())
	scratch := filepath.Clean(r.ScratchRoot)
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return
	}
	for _, e := range entries {
		if _, known := base[e.Name()]; known {
			continue
		}
		path := filepath.Join(tmp, e.Name())
		// Never remove the scratch root itself or a directory containing it.
		if path == scratch || strings.HasPrefix(scratch+string(filepath.Separator), path+string(filepath.Separator)) {
			continue
		}
		if err := os.RemoveAll(path); err == nil {
			removed++
		}
	}
	if removed > 0 {
		log.Printf("runner: between-run cleanup removed %d leftover temp item(s)", removed)
	}
}
