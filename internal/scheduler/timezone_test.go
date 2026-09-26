package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCronMatchesRespectsLocation reproduces the actual bug reported: a
// Schedule Trigger with cron "0 19 * * *" (meant as "7pm local time") must
// fire at 19:00 in the configured zone, not 19:00 UTC. Before SetLocation
// existed, tick() always evaluated cron fields against the instant's UTC
// wall-clock reading, so a Dhaka-intended 19:00 rule actually fired at
// 01:00 the next day in Dhaka (19:00 UTC = 19:00+6 = 01:00 next-day BDT).
func TestCronMatchesRespectsLocation(t *testing.T) {
	dhaka, err := time.LoadLocation("Asia/Dhaka")
	if err != nil {
		t.Fatalf("load Asia/Dhaka: %v", err)
	}

	// 19:00 BDT on a given day is 13:00 UTC the same day (BDT = UTC+6).
	instant := time.Date(2026, 9, 26, 13, 0, 0, 0, time.UTC)
	if got := instant.In(dhaka); got.Hour() != 19 {
		t.Fatalf("sanity check failed: 13:00 UTC should be 19:00 in Asia/Dhaka, got %d:00", got.Hour())
	}

	var mu sync.Mutex
	var fired []string
	run := func(_ context.Context, workflowID, nodeName string) {
		mu.Lock()
		fired = append(fired, workflowID+"/"+nodeName)
		mu.Unlock()
	}

	s := New(run)
	s.Load([]Schedule{{
		ID:         "wf1/Schedule Trigger",
		WorkflowID: "wf1",
		NodeName:   "Schedule Trigger",
		CronExpr:   "0 19 * * *",
		Enabled:    true,
	}})

	// Without SetLocation, the scheduler defaults to UTC (unchanged
	// pre-existing behavior) so 13:00 UTC does NOT match "0 19 * * *".
	s.tick(context.Background(), instant)
	mu.Lock()
	gotBeforeFix := len(fired)
	mu.Unlock()
	if gotBeforeFix != 0 {
		t.Fatalf("expected no run yet (default UTC location), got %d", gotBeforeFix)
	}

	// With the timezone set to Asia/Dhaka, the same 13:00 UTC instant IS
	// 19:00 local time and must fire.
	s.SetLocation(dhaka)
	s.tick(context.Background(), instant)

	// tick() spawns the run in a goroutine; give it a moment.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(fired)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 1 || fired[0] != "wf1/Schedule Trigger" {
		t.Fatalf("expected exactly one run of wf1/Schedule Trigger after SetLocation(Asia/Dhaka), got %v", fired)
	}
}

// TestSetLocationNilIsIgnored ensures a nil location (e.g. a lookup that
// failed) can never leave the scheduler without a usable zone.
func TestSetLocationNilIsIgnored(t *testing.T) {
	s := New(func(context.Context, string, string) {})
	if s.location != time.UTC {
		t.Fatalf("expected default location to be UTC, got %v", s.location)
	}
	s.SetLocation(nil)
	if s.location != time.UTC {
		t.Fatalf("SetLocation(nil) must not change the location, got %v", s.location)
	}
}
