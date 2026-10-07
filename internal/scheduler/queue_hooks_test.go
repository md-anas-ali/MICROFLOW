package scheduler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// Order for two back-to-back jobs:
//
//	prestart -> job1 -> cleanup -> cooldown start/end -> prestart -> job2 -> cleanup
//
// A panicking PreStart must not stop the job (hooks can never veto/kill the queue).
func TestHooksOrderAndPanicIsolation(t *testing.T) {
	s := New(func(ctx context.Context, workflowID, nodeName string) {})
	s.SetCooldown(250 * time.Millisecond)

	var mu sync.Mutex
	var ev []string
	add := func(e string) { mu.Lock(); ev = append(ev, e); mu.Unlock() }
	prestarts := 0
	s.SetCleanup(func(ctx context.Context) { add("cleanup") })
	s.SetHooks(Hooks{
		CooldownStart: func(d time.Duration) { add("cooldown-start") },
		CooldownEnd:   func(interrupted bool) { add("cooldown-end") },
		PreStart: func(ctx context.Context) {
			prestarts++
			add("prestart")
			if prestarts == 1 {
				panic("health check blew up")
			}
		},
	})

	ctx := context.Background()
	if err := s.RunSync(ctx, "svcA", "manual", func(context.Context) { add("job1") }); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s.RunSync(ctx, "svcB", "manual", func(context.Context) { add("job2") }); err != nil {
		t.Fatal(err)
	}
	// cleanup of job2 runs on the worker just after RunSync returns
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	got := strings.Join(ev, ",")
	mu.Unlock()
	want := "prestart,job1,cleanup,cooldown-start,cooldown-end,prestart,job2,cleanup"
	if got != want {
		t.Fatalf("event order\n got: %s\nwant: %s", got, want)
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatal("the second job started before the cooldown elapsed")
	}
}

// With no hooks set the queue behaves exactly as before.
func TestNoHooksUnchanged(t *testing.T) {
	s := New(func(ctx context.Context, workflowID, nodeName string) {})
	ran := false
	if err := s.RunSync(context.Background(), "k", "manual", func(context.Context) { ran = true }); err != nil || !ran {
		t.Fatalf("job did not run: err=%v ran=%v", err, ran)
	}
}
