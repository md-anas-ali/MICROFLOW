package runner

// reaper.go: periodic cleanup of executions that the database still shows
// as queued/running/waiting but that nothing is running any more.
//
// The startup sweep (store.MarkInterruptedExecutions) only fixes rows left
// by a PREVIOUS process. A row can also get stuck while this process stays
// up (a goroutine that died before writing its final status, a lost DB
// write, a run dropped from the queue). Those would show "Running" in the
// monitor until the next restart. The reaper closes them.
//
// Safety rules, all enforced here or in the store query:
//   - rows with a recovery checkpoint are never touched (recovery owns them)
//   - rows younger than the grace period are never touched (a run may be
//     between "row created" and "marked live")
//   - anything this process is running or has queued (isLive) is skipped
//   - the close itself is conditional on the row still being non-terminal
//     (store.FinalizeStaleExecution), so a run that just finished is safe
//
// Relies on the single-instance lock (store.AcquireInstanceLock): with two
// servers on one database, "not live in THIS process" would not mean "dead".

import (
	"context"
	"log"
	"time"
)

// StaleStore is the persistence subset the reaper needs; *store.Store
// satisfies it.
type StaleStore interface {
	ListStaleExecutionIDs(ctx context.Context, startedBefore time.Time, limit int) ([]string, error)
	FinalizeStaleExecution(ctx context.Context, executionID string) (bool, error)
}

// ReapStaleExecutions closes stale rows once and returns how many it
// closed. isLive reports whether this process is running or has queued the
// given execution id.
func ReapStaleExecutions(ctx context.Context, st StaleStore, isLive func(string) bool, grace time.Duration) (int, error) {
	ids, err := st.ListStaleExecutionIDs(ctx, time.Now().Add(-grace), 50)
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		if isLive != nil && isLive(id) {
			continue
		}
		changed, err := st.FinalizeStaleExecution(ctx, id)
		if err != nil {
			log.Printf("reaper: could not close stale execution %s: %v", id, err)
			continue
		}
		if changed {
			closed++
		}
	}
	return closed, nil
}

// StartStaleReaper runs ReapStaleExecutions every interval until ctx ends.
// extra, if non-nil, runs after each pass (used for stale Run-All jobs).
func StartStaleReaper(ctx context.Context, st StaleStore, isLive func(string) bool, extra func(context.Context), interval, grace time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			n, err := ReapStaleExecutions(rctx, st, isLive, grace)
			if err != nil {
				log.Printf("reaper: stale-execution check failed: %v", err)
			} else if n > 0 {
				log.Printf("reaper: closed %d stale execution(s) that nothing was running", n)
			}
			if extra != nil {
				extra(rctx)
			}
			cancel()
		}
	}()
}
