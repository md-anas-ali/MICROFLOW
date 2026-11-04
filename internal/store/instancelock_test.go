package store

// Tests for the single-instance advisory lock. They need a real PostgreSQL:
// set MICROFLOW_TEST_DATABASE_URL (a scratch database; the tests only take
// and release a private advisory lock key and never touch any table).
// Without it they are skipped.

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func lockTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("MICROFLOW_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("MICROFLOW_TEST_DATABASE_URL not set")
	}
	old := lockKey
	lockKey = 0x4D46544553 // private key: never collides with a real server
	t.Cleanup(func() { lockKey = old })
	st, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func advisoryHolders(t *testing.T, st *Store) int {
	t.Helper()
	var n int
	err := st.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_locks
		WHERE locktype='advisory' AND granted AND objsubid=1
		  AND ((classid::bigint << 32) | objid::bigint) = $1`, lockKey).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAnotherInstanceErrorIs(t *testing.T) {
	err := error(&AnotherInstanceError{Holder: "pid=1"})
	if !errors.Is(err, ErrAnotherInstance) {
		t.Fatal("AnotherInstanceError must match ErrAnotherInstance")
	}
	if (&AnotherInstanceError{}).Error() != ErrAnotherInstance.Error() {
		t.Fatal("empty holder must give the plain message")
	}
}

func TestInstanceLockAcquireContendRelease(t *testing.T) {
	st := lockTestStore(t)
	ctx := context.Background()

	l1, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := advisoryHolders(t, st); n != 1 {
		t.Fatalf("holders = %d, want 1", n)
	}

	// Contention: a second server is refused, with holder diagnostics, and
	// the first keeps its lock (never stolen).
	_, err = st.AcquireInstanceLock(ctx, 0)
	var held *AnotherInstanceError
	if !errors.Is(err, ErrAnotherInstance) || !errors.As(err, &held) || held.Holder == "" {
		t.Fatalf("want AnotherInstanceError with holder, got %v", err)
	}
	if n := advisoryHolders(t, st); n != 1 {
		t.Fatalf("lock stolen or lost: holders = %d", n)
	}

	// Release happens on the lock's own session and is idempotent.
	l1.Close()
	l1.Close()
	if n := advisoryHolders(t, st); n != 0 {
		t.Fatalf("lock still held after Close: %d", n)
	}
	var nilLock *InstanceLock
	nilLock.Close()

	l2, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	l2.Close()
}

func TestInstanceLockBoundedWait(t *testing.T) {
	st := lockTestStore(t)
	ctx := context.Background()
	l1, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()

	// Holder never releases: the waiter gives up after ~wait, not forever.
	start := time.Now()
	_, err = st.AcquireInstanceLock(ctx, 3*time.Second)
	el := time.Since(start)
	if !errors.Is(err, ErrAnotherInstance) {
		t.Fatalf("want ErrAnotherInstance, got %v", err)
	}
	if el < 2*time.Second || el > 10*time.Second {
		t.Fatalf("waited %s, want ~3s", el)
	}

	// Holder releases mid-wait (rolling deploy): the waiter then gets it.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		l1.Close()
	}()
	l2, err := st.AcquireInstanceLock(ctx, 15*time.Second)
	if err != nil {
		t.Fatalf("waiter should acquire after release: %v", err)
	}
	l2.Close()
}

func TestInstanceLockShutdownDuringWait(t *testing.T) {
	st := lockTestStore(t)
	l1, err := st.AcquireInstanceLock(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(500 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err = st.AcquireInstanceLock(ctx, time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("shutdown did not interrupt the wait promptly")
	}
	if n := advisoryHolders(t, st); n != 1 {
		t.Fatalf("holder lost its lock: %d", n)
	}
}

func TestInstanceLockReconnectAfterSessionDeath(t *testing.T) {
	st := lockTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var lost atomic.Bool
	l.Watch(ctx, time.Second, func(error) { lost.Store(true) })

	// Kill the lock's own session (simulates a dropped connection).
	if _, err := st.pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype='advisory' AND objsubid=1 AND ((classid::bigint << 32) | objid::bigint) = $1`, lockKey); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if advisoryHolders(t, st) == 1 && l.current() != nil {
			if lost.Load() {
				t.Fatal("onLost fired although nobody else took the lock")
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("lock was not re-acquired after the session died")
}

func TestInstanceLockLostToAnotherServer(t *testing.T) {
	st := lockTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	lostCh := make(chan error, 1)
	l.Watch(ctx, time.Second, func(e error) { lostCh <- e })

	if _, err := st.pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype='advisory' AND objsubid=1 AND ((classid::bigint << 32) | objid::bigint) = $1`, lockKey); err != nil {
		t.Fatal(err)
	}
	// A competitor grabs the freed lock before the watcher notices.
	var other *InstanceLock
	for i := 0; i < 50 && other == nil; i++ {
		other, _ = st.AcquireInstanceLock(ctx, 0)
		time.Sleep(50 * time.Millisecond)
	}
	if other == nil {
		t.Skip("could not win the race for the freed lock")
	}
	defer other.Close()
	select {
	case e := <-lostCh:
		if !errors.Is(e, ErrAnotherInstance) {
			t.Fatalf("onLost err = %v", e)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("onLost not called when another server holds the lock")
	}
	if n := advisoryHolders(t, st); n != 1 {
		t.Fatalf("holders = %d, want 1 (the competitor)", n)
	}
}

func TestInstanceLockWatchStopsOnShutdown(t *testing.T) {
	st := lockTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	l, err := st.AcquireInstanceLock(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	l.Watch(ctx, time.Second, nil)
	cancel() // graceful shutdown: context cancelled, then lock released
	l.Close()
	if n := advisoryHolders(t, st); n != 0 {
		t.Fatalf("lock still held after shutdown: %d", n)
	}
}
