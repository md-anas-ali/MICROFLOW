package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestScheduler() *Scheduler {
	return New(func(context.Context, string, string) {})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Jobs run strictly one at a time, in submission order, even when submitted
// while another is still running.
func TestQueueRunsOneAtATimeInOrder(t *testing.T) {
	s := newTestScheduler()
	var running, maxRunning int32
	var mu sync.Mutex
	var order []string
	release := make(chan struct{})

	mk := func(name string, block bool) func(context.Context) bool {
		return func(context.Context) bool {
			n := atomic.AddInt32(&running, 1)
			for {
				m := atomic.LoadInt32(&maxRunning)
				if n <= m || atomic.CompareAndSwapInt32(&maxRunning, m, n) {
					break
				}
			}
			if block {
				<-release
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			atomic.AddInt32(&running, -1)
			return true
		}
	}

	if err := s.Enqueue("A", "test", mk("A", true)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "A to start", func() bool { return atomic.LoadInt32(&running) == 1 })
	// B and C become due while A is running: they must queue, not start.
	if err := s.Enqueue("B", "test", mk("B", false)); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue("C", "test", mk("C", false)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&running); got != 1 {
		t.Fatalf("expected only A running, got %d running", got)
	}
	close(release)
	waitFor(t, "all three jobs", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Fatalf("expected FIFO order A,B,C got %v", order)
	}
	if m := atomic.LoadInt32(&maxRunning); m != 1 {
		t.Fatalf("max concurrency must be 1, saw %d", m)
	}
}

// The same key can't be queued or run twice; it can be queued again once done.
func TestQueueRejectsDuplicateKey(t *testing.T) {
	s := newTestScheduler()
	started := make(chan struct{})
	release := make(chan struct{})
	if err := s.Enqueue("W", "test", func(context.Context) bool {
		close(started)
		<-release
		return true
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := s.Enqueue("W", "test", func(context.Context) bool { return true }); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("running key must be refused with ErrDuplicate, got %v", err)
	}
	if _, err := s.Reserve("W"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Reserve of running key must fail, got %v", err)
	}
	close(release)
	waitFor(t, "key released", func() bool {
		tk, err := s.Reserve("W")
		if err == nil {
			tk.Release()
			return true
		}
		return false
	})
}

// A failing (panicking) job must not stop the queue; cleanup still runs.
func TestQueueContinuesAfterPanicAndRunsCleanup(t *testing.T) {
	s := newTestScheduler()
	var cleanups int32
	s.SetCleanup(func(context.Context) { atomic.AddInt32(&cleanups, 1) })
	var ranSecond int32
	_ = s.Enqueue("bad", "test", func(context.Context) bool { panic("boom") })
	_ = s.Enqueue("good", "test", func(context.Context) bool { atomic.StoreInt32(&ranSecond, 1); return true })
	waitFor(t, "second job after panic", func() bool { return atomic.LoadInt32(&ranSecond) == 1 })
	waitFor(t, "cleanup after both jobs", func() bool { return atomic.LoadInt32(&cleanups) == 2 })
}

// A job that reports it did not run (returns false) gets no cleanup/cooldown.
func TestQueueNoCleanupWhenNothingRan(t *testing.T) {
	s := newTestScheduler()
	var cleanups int32
	s.SetCleanup(func(context.Context) { atomic.AddInt32(&cleanups, 1) })
	done := make(chan struct{})
	_ = s.Enqueue("skip", "test", func(context.Context) bool { close(done); return false })
	<-done
	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt32(&cleanups); n != 0 {
		t.Fatalf("cleanup must not run when nothing ran, ran %d times", n)
	}
}

// The next job starts no earlier than cooldown after the previous one ended.
func TestQueueCooldownBetweenJobs(t *testing.T) {
	s := newTestScheduler()
	s.SetCooldown(150 * time.Millisecond)
	var mu sync.Mutex
	var firstEnd, secondStart time.Time
	_ = s.Enqueue("A", "test", func(context.Context) bool {
		mu.Lock()
		firstEnd = time.Now()
		mu.Unlock()
		return true
	})
	_ = s.Enqueue("B", "test", func(context.Context) bool {
		mu.Lock()
		secondStart = time.Now()
		mu.Unlock()
		return true
	})
	waitFor(t, "B to start", func() bool { mu.Lock(); defer mu.Unlock(); return !secondStart.IsZero() })
	mu.Lock()
	defer mu.Unlock()
	if gap := secondStart.Sub(firstEnd); gap < 140*time.Millisecond {
		t.Fatalf("expected ~150ms cooldown between jobs, got %s", gap)
	}
}

// RunSync blocks until its turn and result, and a caller whose ctx ends while
// still queued is withdrawn without ever running.
func TestRunSyncQueuesAndWithdraws(t *testing.T) {
	s := newTestScheduler()
	started := make(chan struct{})
	release := make(chan struct{})
	_ = s.Enqueue("A", "test", func(context.Context) bool {
		close(started)
		<-release
		return true
	})
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	var ran int32
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.RunSync(ctx, "B", "test", func(context.Context) { atomic.StoreInt32(&ran, 1) })
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled for withdrawn job, got %v", err)
	}
	close(release)
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&ran) != 0 {
		t.Fatal("withdrawn job must never run")
	}
	// Key is free again after withdrawal.
	var ok int32
	if err := s.RunSync(context.Background(), "B", "test", func(context.Context) { atomic.StoreInt32(&ok, 1) }); err != nil || ok != 1 {
		t.Fatalf("RunSync after withdrawal failed: err=%v ran=%d", err, ok)
	}
}

// A schedule deactivated while its job waits in the queue is dropped, not run.
func TestScheduledJobDroppedWhenDeactivatedWhileQueued(t *testing.T) {
	var fired int32
	s := New(func(context.Context, string, string) { atomic.AddInt32(&fired, 1) })
	s.Load([]Schedule{{ID: "wf/T", WorkflowID: "wf", NodeName: "T", IntervalSeconds: 60, Enabled: true}})
	job := s.scheduledJob(Schedule{ID: "wf/T", WorkflowID: "wf", NodeName: "T"})
	s.ReplaceWorkflow("wf", []Schedule{{ID: "wf/T", WorkflowID: "wf", NodeName: "T", IntervalSeconds: 60, Enabled: false}})
	if job(context.Background()) {
		t.Fatal("deactivated schedule must not report that it ran")
	}
	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("deactivated schedule must not run")
	}
}
