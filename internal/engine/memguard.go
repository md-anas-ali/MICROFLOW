package engine

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// MemGuard periodically checks Go's own heap usage against a configured
// ceiling (the operator sets this a good margin below the container's
// 512MB total, leaving room for the Go runtime itself, the frontend's
// static assets being served, FFmpeg/TTS child processes, and OS
// overhead). When over the soft ceiling, ShouldThrottle() returns true
// and callers (the engine's heavy-work gate, the scheduler) should defer
// starting new FFmpeg/TTS/HTTP-heavy work rather than launching more
// (rule 19) -- existing required work is still allowed to finish, never
// silently skipped.
type MemGuard struct {
	softCeilingBytes uint64
	throttled        atomic.Bool
}

func NewMemGuard(softCeilingBytes uint64) *MemGuard {
	return &MemGuard{softCeilingBytes: softCeilingBytes}
}

// Start polls memory stats every 5s (cheap: runtime.ReadMemStats does
// not itself trigger a GC) until stop is closed.
func (g *MemGuard) Start(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			g.throttled.Store(m.HeapAlloc >= g.softCeilingBytes)
		}
	}
}

// ShouldThrottle reports whether heap usage was over the soft ceiling as
// of the last poll. Safe to call on a nil *MemGuard (returns false) so
// callers that don't wire a guard (tests, alternate entrypoints) don't
// need nil checks of their own.
func (g *MemGuard) ShouldThrottle() bool {
	if g == nil {
		return false
	}
	return g.throttled.Load()
}

// WaitIfThrottled is the piece that was previously missing: every call
// site that is about to start new heavy work (FFmpeg/TTS child process,
// outbound HTTP, a new Code-node JS VM) calls this first. If the guard
// is currently over ceiling it backs off in short increments, giving the
// GC and in-flight work a chance to bring HeapAlloc back down, instead
// of piling on more concurrent allocation. It NEVER drops the work --
// required behavior (rule: don't skip nodes/features) -- it only delays
// the start, and gives up waiting after maxWait so a persistently full
// heap degrades to "slower" rather than "execution silently never
// starts". Returns early if ctx is cancelled.
func (g *MemGuard) WaitIfThrottled(ctx context.Context) {
	if g == nil || !g.ShouldThrottle() {
		return
	}
	const (
		step    = 250 * time.Millisecond
		maxWait = 5 * time.Second
	)
	deadline := time.Now().Add(maxWait)
	for g.ShouldThrottle() && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(step):
		}
	}
}
