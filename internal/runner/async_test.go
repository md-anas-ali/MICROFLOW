package runner

import (
	"testing"
	"time"
)

// TestBroadcasterReplay_BoundedLenAndCap confirms publish() trims the
// replay buffer to a fresh, exactly-sized slice once past
// replayBufSize, not a re-slice of a larger backing array. A plain
// b.replay[extra:] re-slice keeps the SAME backing array alive (Go's
// GC retains a backing array as a whole for as long as any live slice
// points into it, regardless of len/offset), so every "dropped" older
// Event -- each potentially carrying a multi-MB NodeRunResult (e.g. a
// QC vision request's base64 frames, see internal/nodes/code.go's
// $readFileBase64) -- would stay reachable, and therefore
// un-collectible, until Go's append() growth strategy happens to
// reallocate. Asserting cap(replay) == len(replay) == replayBufSize
// after many publishes is exactly what distinguishes "fresh copy" from
// "re-sliced view into a bigger array": a re-sliced view's cap keeps
// shrinking by 1 per call (from the growing backing array) rather than
// staying pinned at replayBufSize.
func TestBroadcasterReplay_BoundedLenAndCap(t *testing.T) {
	b := newBroadcaster()
	for i := 0; i < replayBufSize*5; i++ {
		b.publish(Event{Type: EventNodeCompleted, Time: time.Now()})
	}
	b.mu.Lock()
	gotLen := len(b.replay)
	gotCap := cap(b.replay)
	b.mu.Unlock()

	if gotLen != replayBufSize {
		t.Fatalf("replay len = %d, want %d", gotLen, replayBufSize)
	}
	// A re-sliced (not copied) trim would leave cap strictly greater
	// than len once the backing array has grown past replayBufSize --
	// exactly the leak this test guards against.
	if gotCap != replayBufSize {
		t.Fatalf("replay cap = %d, want %d (a larger cap means the old backing array -- and every trimmed Event's NodeRunResult data -- is still being kept alive)", gotCap, replayBufSize)
	}
}

// TestBroadcasterReplay_ProcessWideNeverEvicted is a smaller sanity
// check that a broadcaster reused across many more publishes than its
// window size (modeling Manager.all, the process-wide broadcaster that
// is never evicted for the life of the server -- see async.go's
// publish call site) still only ever exposes the most recent window
// via subscribe(), regardless of how long the process has been running.
func TestBroadcasterReplay_ProcessWideNeverEvicted(t *testing.T) {
	b := newBroadcaster()
	const total = replayBufSize * 100
	for i := 0; i < total; i++ {
		b.publish(Event{Type: EventNodeCompleted, Time: time.Now()})
	}
	_, _, replay := b.subscribe()
	if len(replay) != replayBufSize {
		t.Fatalf("replay window = %d entries after %d publishes, want %d", len(replay), total, replayBufSize)
	}
}
