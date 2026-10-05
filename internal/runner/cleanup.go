package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// junkTempPatterns are the leftover working files the workflow's shell/Python
// steps write straight into the OS temp dir (hardcoded /tmp/... paths, outside
// the per-execution scratch dir), so nothing removes them when a run ends.
// Only these names are ever deleted. Deliberately NOT listed (kept so the next
// run does not have to re-download/rebuild them): /tmp/fonts, caption_font.json,
// edge_voices_cache.json and the TTS lock file.
var junkTempPatterns = []string{
	"scene_*", "clip_*", "final*.mp4", "merged*.mp4", "withaudio.mp4",
	"full.ass", "repaired_audio.m4a", "thumb*.jpg", "srt_lines.txt",
	"audio_concat.txt", "concat.txt", "bgm.mp3", "last_subtitle_error.log",
	"ffmpeg_err_*.log", "render_start", "voice_choice.txt", "tts_text_*",
}

// CleanupJunk removes everything a finished Service run leaves behind that no
// later run needs, so the next Service starts clean. It is called by the global
// scheduler after every job (success, error or cancel), before the cooldown.
//
// It NEVER touches persistent data (workflows, nodes, prompts, Environment,
// Credentials, configs, the database). It only deletes:
//  1. the scratch directory of finished runs (waits for the async removal),
//  2. stale scratch directories nobody is running (older than
//     MICROFLOW_STALE_SCRATCH_MINUTES, default 30),
//  3. the known workflow temp files above in the OS temp dir.
//
// If any execution is still live in this process, the file sweeps (2, 3) are
// skipped so a running workflow's files are never pulled out from under it.
// MICROFLOW_TMP_CLEAN=0 disables 2 and 3; MICROFLOW_TMP_CLEAN_EXTRA adds
// comma-separated glob patterns (matched against file names in the temp dir).
func (r *Runner) CleanupJunk(ctx context.Context) {
	r.WaitScratchCleanup(ctx)
	if strings.TrimSpace(os.Getenv("MICROFLOW_TMP_CLEAN")) == "0" {
		return
	}

	r.liveExecMu.Lock()
	live := len(r.liveExecs)
	liveSet := make(map[string]struct{}, live)
	for id := range r.liveExecs {
		liveSet[id] = struct{}{}
	}
	r.liveExecMu.Unlock()
	if live > 0 {
		log.Printf("cleanup: %d execution(s) still running, skipping temp-file sweep", live)
		return
	}

	removed := 0

	// 2) stale per-execution scratch directories left by crashed/killed runs.
	if r.ScratchRoot != "" {
		maxAge := 30 * time.Minute
		if v := os.Getenv("MICROFLOW_STALE_SCRATCH_MINUTES"); v != "" {
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n >= 1 {
				maxAge = time.Duration(n) * time.Minute
			}
		}
		if entries, err := os.ReadDir(r.ScratchRoot); err == nil {
			for _, e := range entries {
				if ctx.Err() != nil {
					return
				}
				if _, isLive := liveSet[e.Name()]; isLive {
					continue
				}
				info, err := e.Info()
				if err != nil || time.Since(info.ModTime()) < maxAge {
					continue
				}
				if os.RemoveAll(filepath.Join(r.ScratchRoot, e.Name())) == nil {
					removed++
				}
			}
		}
	}

	// 3) workflow temp files in the OS temp dir (/tmp is hardcoded by the
	// workflow, so always include it besides os.TempDir()).
	patterns := append([]string(nil), junkTempPatterns...)
	for _, p := range strings.Split(os.Getenv("MICROFLOW_TMP_CLEAN_EXTRA"), ",") {
		if p = strings.TrimSpace(p); p != "" && !strings.ContainsAny(p, `/\`) {
			patterns = append(patterns, p)
		}
	}
	dirs := []string{"/tmp"}
	if td := os.TempDir(); td != "/tmp" {
		dirs = append(dirs, td)
	}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if ctx.Err() != nil {
				return
			}
			if !e.Type().IsRegular() { // never follow symlinks or remove directories
				continue
			}
			for _, p := range patterns {
				if ok, _ := filepath.Match(p, e.Name()); ok {
					if os.Remove(filepath.Join(dir, e.Name())) == nil {
						removed++
					}
					break
				}
			}
		}
	}
	if removed > 0 {
		log.Printf("cleanup: removed %d leftover temp item(s) after the run", removed)
	}
}
