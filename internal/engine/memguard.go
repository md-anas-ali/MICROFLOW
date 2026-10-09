package engine

import (
	"context"
	"log"
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
	// giveUpUntil (unix nanoseconds) is set when a full maxWait did not bring
	// the heap back under the ceiling. Until then WaitIfThrottled returns
	// immediately instead of making every following node wait again.
	giveUpUntil atomic.Int64
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

// Reset forgets everything the guard learned during the previous Service: the
// "give up waiting" back-off window is cleared and the throttle flag is
// re-evaluated against the heap as it is NOW (call it right after a GC), so the
// next Service never starts throttled or skipping waits because of the last
// one's memory peak. Safe on a nil *MemGuard.
func (g *MemGuard) Reset() {
	if g == nil {
		return
	}
	g.giveUpUntil.Store(0)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	g.throttled.Store(m.HeapAlloc >= g.softCeilingBytes)
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
	// A previous full wait already showed that the heap is not coming down
	// (the live heap itself is above the ceiling, not just transient
	// garbage). Waiting again cannot help, and a workflow that runs hundreds
	// of Code/HTTP/Command nodes would otherwise pay up to maxWait for every
	// single one of them -- minutes become hours. Skip the wait until the
	// back-off window ends, then probe again.
	if time.Now().UnixNano() < g.giveUpUntil.Load() {
		return
	}
	deadline := time.Now().Add(maxWait)
	for g.ShouldThrottle() && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(step):
		}
	}
	if g.ShouldThrottle() {
		g.giveUpUntil.Store(time.Now().Add(giveUpBackoff).UnixNano())
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[MemGuard] heap %d MB is still over the %d MB ceiling after waiting %s; "+
			"continuing without further waits for %s (the live heap is above the ceiling -- "+
			"raise MICROFLOW_HEAP_CEILING_MB if this repeats)",
			m.HeapAlloc>>20, g.softCeilingBytes>>20, maxWait, giveUpBackoff)
	}
}

// giveUpBackoff is how long WaitIfThrottled stops waiting after a full wait
// failed to bring the heap under the ceiling.
const giveUpBackoff = 30 * time.Second
