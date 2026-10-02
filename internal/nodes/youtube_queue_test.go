package nodes

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

var testDhaka = time.FixedZone("Asia/Dhaka", 6*60*60)

func testCfg() pubConfig {
	return pubConfig{
		Loc: testDhaka, Slots: []slotHM{{7, 0}, {22, 0}},
		MinLead: 2 * time.Minute, StaleAfter: 45 * time.Minute, ScanMax: 500, MaxMoves: 25,
	}
}

// dk builds an instant on day d of October 2026, Asia/Dhaka local time.
func dk(d, h, m int) time.Time { return time.Date(2026, 10, d, h, m, 0, 0, testDhaka).UTC() }

func reserveAt(t *testing.T, st *pubState, now time.Time, op string, snap []schedVideo) reserveResult {
	t.Helper()
	r, err := st.reserve(testCfg(), reserveInput{OpKey: op, Title: "t-" + op, Now: now, Snapshot: snap, SnapshotOK: true})
	if err != nil {
		t.Fatalf("reserve(%s): %v", op, err)
	}
	return r
}

func assertSlot(t *testing.T, name string, got, want time.Time) {
	t.Helper()
	if !got.Equal(want) {
		t.Fatalf("%s: got %s, want %s", name, got.In(testDhaka).Format(time.RFC3339), want.In(testDhaka).Format(time.RFC3339))
	}
}

func assertDayCap(t *testing.T, slots []time.Time) {
	t.Helper()
	cal := slotCalendar{loc: testDhaka, slots: testCfg().Slots}
	seen := map[int64]bool{}
	perDay := map[string]int{}
	for _, s := range slots {
		if seen[s.Unix()] {
			t.Fatalf("slot %s assigned twice", s.In(testDhaka).Format(time.RFC3339))
		}
		seen[s.Unix()] = true
		if !cal.isSlot(s) {
			t.Fatalf("%s is not on the 07:00/22:00 grid", s.In(testDhaka).Format(time.RFC3339))
		}
		perDay[cal.dayKey(s)]++
		if perDay[cal.dayKey(s)] > 2 {
			t.Fatalf("more than 2 videos on %s", cal.dayKey(s))
		}
	}
}

// 1. Ready at 21:50 -> tonight 22:00.
func TestPubQueue_BeforeTenPM(t *testing.T) {
	var st pubState
	assertSlot(t, "21:50", reserveAt(t, &st, dk(2, 21, 50), "a", nil).Slot, dk(2, 22, 0))
}

// 2. Ready at 22:05 -> next day 07:00.
func TestPubQueue_AfterTenPM(t *testing.T) {
	var st pubState
	assertSlot(t, "22:05", reserveAt(t, &st, dk(2, 22, 5), "a", nil).Slot, dk(3, 7, 0))
}

// A slot that already passed, or is closer than the minimum lead, is never used.
func TestPubQueue_PassedOrTooCloseSlotSkipped(t *testing.T) {
	var st pubState
	assertSlot(t, "21:59 (lead 2m)", reserveAt(t, &st, dk(2, 21, 59), "a", nil).Slot, dk(3, 7, 0))
	var st2 pubState
	assertSlot(t, "07:01", reserveAt(t, &st2, dk(2, 7, 1), "a", nil).Slot, dk(2, 22, 0))
}

// 3. Five videos at once -> five consecutive free slots, in queue order.
func TestPubQueue_FiveAtOnce(t *testing.T) {
	var st pubState
	now := dk(2, 21, 50)
	want := []time.Time{dk(2, 22, 0), dk(3, 7, 0), dk(3, 22, 0), dk(4, 7, 0), dk(4, 22, 0)}
	var got []time.Time
	for i, op := range []string{"a", "b", "c", "d", "e"} {
		r := reserveAt(t, &st, now, op, nil)
		assertSlot(t, op, r.Slot, want[i])
		got = append(got, r.Slot)
	}
	assertDayCap(t, got)
}

// 4 + 5. Messy existing schedule is normalized: order preserved, one
// video per slot, max 2 per day; the new video goes after them.
func TestPubQueue_ReconcileExisting(t *testing.T) {
	var st pubState
	now := dk(2, 20, 0)
	snap := []schedVideo{
		{ID: "G", PublishAt: dk(3, 22, 0)},
		{ID: "A", PublishAt: dk(2, 22, 0)},
		{ID: "B", PublishAt: dk(2, 21, 13)}, // off-grid
		{ID: "C", PublishAt: dk(3, 7, 0)},
		{ID: "D", PublishAt: dk(3, 7, 0)},    // duplicate of C
		{ID: "E", PublishAt: dk(3, 8, 30)},   // off-grid
		{ID: "F", PublishAt: dk(3, 22, 0)},   // duplicate of G
	}
	r := reserveAt(t, &st, now, "new", snap)
	want := map[string]time.Time{
		"B": dk(2, 22, 0), "A": dk(3, 7, 0), "C": dk(3, 22, 0), "D": dk(4, 7, 0),
		"E": dk(4, 22, 0), "F": dk(5, 7, 0), "G": dk(5, 22, 0),
	}
	if len(r.Moves) != len(want) {
		t.Fatalf("expected %d moves, got %d", len(want), len(r.Moves))
	}
	var all []time.Time
	var prev time.Time
	for _, m := range r.Moves { // moves come back in chronological (old-time) order
		assertSlot(t, "move "+m.VideoID, m.To, want[m.VideoID])
		if !prev.IsZero() && !m.To.After(prev) {
			t.Fatalf("order not preserved at %s", m.VideoID)
		}
		prev = m.To
		all = append(all, m.To)
	}
	assertSlot(t, "new video after existing", r.Slot, dk(6, 7, 0))
	assertDayCap(t, append(all, r.Slot))
}

// Already-compliant schedules cause zero API moves.
func TestPubQueue_CompliantScheduleUntouched(t *testing.T) {
	var st pubState
	snap := []schedVideo{{ID: "A", PublishAt: dk(2, 22, 0)}, {ID: "B", PublishAt: dk(3, 7, 0)}}
	r := reserveAt(t, &st, dk(2, 20, 0), "new", snap)
	if len(r.Moves) != 0 || r.Kept != 2 {
		t.Fatalf("moves=%d kept=%d, want 0/2", len(r.Moves), r.Kept)
	}
	assertSlot(t, "new", r.Slot, dk(3, 22, 0))
}

// A video about to publish is left alone but still counts toward its day.
func TestPubQueue_ImminentVideoCountsTowardDayCap(t *testing.T) {
	var st pubState
	now := dk(2, 20, 0)
	snap := []schedVideo{{ID: "X1", PublishAt: dk(2, 20, 1)}}
	r1 := reserveAt(t, &st, now, "a", snap)
	assertSlot(t, "a", r1.Slot, dk(2, 22, 0)) // day total becomes 2
	r2 := reserveAt(t, &st, now, "b", snap)
	assertSlot(t, "b", r2.Slot, dk(3, 7, 0)) // day 2 is full
	if len(r1.Moves) != 0 {
		t.Fatalf("imminent video must not be moved")
	}
	// Two imminent videos on the same instant: day is already full.
	var st2 pubState
	snap2 := []schedVideo{{ID: "X1", PublishAt: dk(2, 20, 1)}, {ID: "X2", PublishAt: dk(2, 20, 1)}}
	assertSlot(t, "full day", reserveAt(t, &st2, now, "a", snap2).Slot, dk(3, 7, 0))
}

// 6. Retry / restart idempotency.
func TestPubQueue_IdempotentRetryAndCommit(t *testing.T) {
	var st pubState
	now := dk(2, 21, 50)
	r1 := reserveAt(t, &st, now, "op", nil)
	r2 := reserveAt(t, &st, now.Add(10*time.Second), "op", nil) // retry before commit
	assertSlot(t, "retry same slot", r2.Slot, r1.Slot)
	if len(st.Entries) != 1 {
		t.Fatalf("retry created %d entries, want 1", len(st.Entries))
	}
	st.commit("op", "VID1", fmtPubTime(now))
	r3 := reserveAt(t, &st, now.Add(time.Minute), "op", nil) // resume after finished upload
	if !r3.AlreadyDone || r3.VideoID != "VID1" {
		t.Fatalf("expected AlreadyDone with VID1, got %+v", r3)
	}
	if len(st.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(st.Entries))
	}
	// Failed upload releases the slot; the retry re-uses the same entry.
	var st2 pubState
	reserveAt(t, &st2, now, "x", nil)
	st2.release("x", "boom", fmtPubTime(now))
	r4 := reserveAt(t, &st2, now.Add(time.Second), "x", nil)
	assertSlot(t, "after release", r4.Slot, dk(2, 22, 0))
	if len(st2.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(st2.Entries))
	}
}

// A retry whose previous attempt actually uploaded (response lost)
// adopts the video instead of uploading a duplicate.
func TestPubQueue_AdoptsLostResponseUpload(t *testing.T) {
	var st pubState
	now := dk(2, 21, 50)
	r1 := reserveAt(t, &st, now, "op", nil)
	snap := []schedVideo{{ID: "V1", Title: "t-op", PublishAt: r1.Slot}}
	r2 := reserveAt(t, &st, now.Add(30*time.Second), "op", snap)
	if !r2.Adopted || r2.VideoID != "V1" {
		t.Fatalf("expected adoption of V1, got %+v", r2)
	}
	if len(st.Entries) != 1 || st.Entries[0].VideoID != "V1" {
		t.Fatalf("unexpected entries: %+v", st.Entries)
	}
}

// Server restart: state is persisted as plain JSON inside static data.
func TestPubQueue_StateSurvivesRoundTrip(t *testing.T) {
	var st pubState
	now := dk(2, 21, 50)
	r1 := reserveAt(t, &st, now, "a", nil)
	data := map[string]any{}
	if err := st.save(data); err != nil {
		t.Fatal(err)
	}
	st2, err := loadPubState(data)
	if err != nil {
		t.Fatal(err)
	}
	r2 := reserveAt(t, &st2, now, "b", nil)
	if r2.Slot.Equal(r1.Slot) {
		t.Fatalf("slot reused after reload: %s", r2.Slot)
	}
	assertSlot(t, "second video", r2.Slot, dk(3, 7, 0))
}

type memStatic struct {
	mu   sync.Mutex
	data map[string]any
}

func (m *memStatic) WithLock(_ context.Context, _ string, fn func(map[string]any) (map[string]any, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := map[string]any{}
	for k, v := range m.data {
		cp[k] = v
	}
	nd, err := fn(cp)
	if err != nil {
		return err
	}
	m.data = nd
	return nil
}

// Concurrent executions never get the same slot.
func TestPubQueue_ConcurrentReservationsUnique(t *testing.T) {
	q := &pubQueue{store: &memStatic{}, workflowID: "wf", cfg: testCfg(), now: func() time.Time { return dk(2, 21, 50) }}
	const n = 30
	slots := make([]time.Time, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := q.mutate(context.Background(), func(st *pubState) error {
				r, e := st.reserve(q.cfg, reserveInput{OpKey: fmt.Sprintf("op-%d", i), Title: "t", Now: q.now(), SnapshotOK: true})
				slots[i] = r.Slot
				return e
			})
			if err != nil {
				t.Errorf("mutate: %v", err)
			}
		}(i)
	}
	wg.Wait()
	assertDayCap(t, slots)
}
