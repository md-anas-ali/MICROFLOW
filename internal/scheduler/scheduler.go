// Package scheduler is a lightweight in-process scheduler (rule 14: no
// Redis/Kafka needed). It wakes once a minute (stdlib time.Ticker,
// no busy-loop -- rule 20), checks which schedules are due, and hands
// matching workflow+node pairs to a Runner callback.
package scheduler

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Schedule mirrors one Schedule Trigger node's config: either a cron
// expression or a simple interval, per rule 14.
type Schedule struct {
	ID              string
	WorkflowID      string
	NodeName        string
	CronExpr        string // 5-field standard cron: minute hour day month weekday
	IntervalSeconds int
	Enabled         bool
}

// Runner is called when a schedule fires. Implemented by the API/engine
// glue (internal/api) to kick off engine.Run for that workflow.
type Runner func(ctx context.Context, workflowID, nodeName string)

type Scheduler struct {
	// mu guards schedules and lastRun: ReplaceWorkflow (called from the
	// workflow save/import/delete API path) mutates them while tick runs
	// on the scheduler goroutine.
	mu        sync.Mutex
	schedules []Schedule
	run       Runner
	lastRun   map[string]time.Time

	// inFlight guards against overlapping runs of the *same* schedule: if
	// a Schedule Trigger's workflow (e.g. a multi-minute FFmpeg render
	// pipeline) is still running when the next minute-tick sees it's due
	// again, we skip that tick rather than stacking another concurrent
	// run on top (rule 20: bounded concurrency -- this was previously
	// unbounded, since every due tick unconditionally spawned a new
	// goroutine regardless of whether the prior run for that schedule had
	// finished). The existing run is never interrupted; we only decline
	// to start a second one until the first returns.
	inFlightMu sync.Mutex
	inFlight   map[string]bool
}

func New(run Runner) *Scheduler {
	return &Scheduler{run: run, lastRun: map[string]time.Time{}, inFlight: map[string]bool{}}
}

func (s *Scheduler) Load(schedules []Schedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = schedules
}

// ReplaceWorkflow atomically swaps every schedule belonging to
// workflowID for the given set (nil/empty removes them all, e.g. after
// the workflow is deleted). Called after a workflow is saved so the
// running scheduler reflects the persisted definition without a
// restart. Because the workflow's old entries are dropped before the new
// ones are added, the same schedule can never be registered twice.
// lastRun/inFlight are keyed by schedule ID and deliberately kept, so an
// in-progress run still blocks an overlapping tick (overlap protection is
// unchanged). An interval schedule that is newly enabled, or whose
// interval changed, starts counting from now instead of firing on the
// next tick.
func (s *Scheduler) ReplaceWorkflow(workflowID string, schedules []Schedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := make(map[string]Schedule)
	kept := make([]Schedule, 0, len(s.schedules)+len(schedules))
	for _, sc := range s.schedules {
		if sc.WorkflowID == workflowID {
			old[sc.ID] = sc
			continue
		}
		kept = append(kept, sc)
	}
	now := time.Now()
	for _, sc := range schedules {
		if sc.WorkflowID != workflowID {
			continue
		}
		prev, existed := old[sc.ID]
		if sc.Enabled && sc.CronExpr == "" && sc.IntervalSeconds > 0 &&
			(!existed || !prev.Enabled || prev.IntervalSeconds != sc.IntervalSeconds) {
			s.lastRun[sc.ID] = now
		}
		kept = append(kept, sc)
	}
	s.schedules = kept
}

// Start blocks until ctx is cancelled, ticking once a minute. A single
// ticker for every schedule (not one goroutine/timer per schedule)
// keeps the scheduler's own footprint flat regardless of how many
// Schedule Trigger nodes exist (rule 20: no unnecessary workers).
func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	s.tick(ctx, time.Now()) // fire immediately on startup for interval schedules already due
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.tick(ctx, now)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sc := range s.schedules {
		if !sc.Enabled {
			continue
		}
		due := false
		switch {
		case sc.CronExpr != "":
			due = cronMatches(sc.CronExpr, now)
		case sc.IntervalSeconds > 0:
			last := s.lastRun[sc.ID]
			due = now.Sub(last) >= time.Duration(sc.IntervalSeconds)*time.Second
		}
		if due {
			s.lastRun[sc.ID] = now

			s.inFlightMu.Lock()
			if s.inFlight[sc.ID] {
				s.inFlightMu.Unlock()
				log.Printf("scheduler: %s/%s still running from a previous tick, skipping this one", sc.WorkflowID, sc.NodeName)
				continue
			}
			s.inFlight[sc.ID] = true
			s.inFlightMu.Unlock()

			go func(sc Schedule) {
				defer func() {
					s.inFlightMu.Lock()
					delete(s.inFlight, sc.ID)
					s.inFlightMu.Unlock()
					if r := recover(); r != nil {
						log.Printf("scheduler: run for %s/%s panicked: %v", sc.WorkflowID, sc.NodeName, r)
					}
				}()
				s.run(ctx, sc.WorkflowID, sc.NodeName)
			}(sc)
		}
	}
}

// cronMatches implements standard 5-field cron (minute hour day month
// weekday) with '*' and comma lists and '*/n' steps -- enough for the
// workflow's Schedule Trigger / Fallback Trigger nodes. Deliberately not
// a full cron library to avoid an extra dependency for a small feature
// (rule 20: no unnecessary deps in a RAM-constrained build).
func cronMatches(expr string, t time.Time) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	return fieldMatches(fields[0], t.Minute(), 0, 59) &&
		fieldMatches(fields[1], t.Hour(), 0, 23) &&
		fieldMatches(fields[2], t.Day(), 1, 31) &&
		fieldMatches(fields[3], int(t.Month()), 1, 12) &&
		fieldMatches(fields[4], int(t.Weekday()), 0, 6)
}

func fieldMatches(field string, value, min, max int) bool {
	if field == "*" {
		return true
	}
	for _, part := range strings.Split(field, ",") {
		if strings.HasPrefix(part, "*/") {
			step, err := strconv.Atoi(part[2:])
			if err != nil || step <= 0 {
				continue
			}
			if (value-min)%step == 0 {
				return true
			}
			continue
		}
		if n, err := strconv.Atoi(part); err == nil && n == value {
			return true
		}
	}
	return false
}

func (s *Schedule) String() string {
	return fmt.Sprintf("%s/%s", s.WorkflowID, s.NodeName)
}
