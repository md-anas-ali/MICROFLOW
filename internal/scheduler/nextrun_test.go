package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNextCronTimeMatchesCronMatches(t *testing.T) {
	loc := time.UTC
	from := time.Date(2026, 9, 30, 10, 7, 0, 0, loc)
	for _, expr := range []string{"0 19 * * *", "30 9 * * 1,3,5", "*/15 * * * *", "0 0 1 * *", "45 23 * * 0"} {
		got, ok := nextCronTime(expr, from)
		if !ok {
			t.Fatalf("%q: no next run", expr)
		}
		if got.Before(from) || !cronMatches(expr, got) {
			t.Fatalf("%q: bad next %v", expr, got)
		}
		// nothing earlier may match
		for m := from; m.Before(got); m = m.Add(time.Minute) {
			if cronMatches(expr, m) {
				t.Fatalf("%q: skipped earlier match %v (got %v)", expr, m, got)
			}
		}
	}
}

func TestNextRunsWindowAndRepeat(t *testing.T) {
	s := New(nil)
	now := time.Date(2026, 9, 30, 10, 0, 30, 0, time.UTC)
	s.Load([]Schedule{
		{ID: "a", RunAll: true, Enabled: true, CronExpr: "0 19 * * *"},
		{ID: "b", RunAll: true, Enabled: true, CronExpr: "0 19 * * *", StartAt: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)},
		{ID: "c", RunAll: true, Enabled: true, CronExpr: "0 19 * * *", EndAt: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)},
		{ID: "d", RunAll: true, Enabled: false, MaxRuns: 2, RunCount: 2, CronExpr: "0 19 * * *"},
		{ID: "e", RunAll: true, Enabled: false, CronExpr: "0 19 * * *"},
	})
	got := map[string]NextRunInfo{}
	for _, r := range s.NextRuns(now) {
		got[r.ID] = r
	}
	if !got["a"].Next.Equal(time.Date(2026, 9, 30, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("a: %v", got["a"])
	}
	if !got["b"].Next.Equal(time.Date(2026, 10, 5, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("b: %v", got["b"])
	}
	if got["c"].State != "ended" || got["d"].State != "completed" || got["e"].State != "disabled" {
		t.Fatalf("states: c=%s d=%s e=%s", got["c"].State, got["d"].State, got["e"].State)
	}
}

func TestTickStopsAfterMaxRuns(t *testing.T) {
	s := New(nil)
	var fired atomic.Int32
	s.SetRunAllRunner(func(_ context.Context, _ bool) error { fired.Add(1); return nil })
	s.Load([]Schedule{{ID: "x", RunAll: true, Enabled: true, IntervalSeconds: 60, MaxRuns: 1}})
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	s.tick(context.Background(), base)
	s.tick(context.Background(), base.Add(2*time.Minute))
	time.Sleep(50 * time.Millisecond)
	if n := fired.Load(); n != 1 {
		t.Fatalf("fired %d times, want 1", n)
	}
}
