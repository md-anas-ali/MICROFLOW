// Package scheduler is a lightweight in-process scheduler (rule 14: no
// Redis/Kafka needed). It wakes once a minute, on the minute (stdlib
// time.Timer, no busy-loop -- rule 20), checks which schedules are due, and hands
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
	// An enabled interval schedule restored at startup counts its first
	// interval from now (same as ReplaceWorkflow does for a newly enabled
	// one) instead of firing the moment the process boots -- otherwise every
	// restart/redeploy would trigger an extra, off-schedule run.
	now := time.Now()
	for _, sc := range schedules {
		if sc.Enabled && sc.CronExpr == "" && sc.IntervalSeconds > 0 {
			s.lastRun[sc.ID] = now
		}
	}
}

// ReplaceWorkflow atomically swaps every schedule belonging to
// workflowID for the given set (nil/empty removes them all, e.g. after
// the workflow is deleted). Called after a workflow is saved so the
// running scheduler reflects the persisted definition without a
// restart. Because the workflow's old entries are dropped before the new
// ones are added, the same schedule can never be registered twice.
// lastRun is keyed by schedule ID and inFlight by workflow/node, and both
// are deliberately kept, so an in-progress run still blocks an overlapping
// tick (overlap protection is unchanged). An interval schedule that is newly enabled, or whose
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
	registered := make(map[string]bool, len(schedules))
	for _, sc := range schedules {
		if sc.WorkflowID != workflowID {
			continue
		}
		registered[sc.ID] = true
		prev, existed := old[sc.ID]
		changed := !existed || !prev.Enabled || prev.CronExpr != sc.CronExpr || prev.IntervalSeconds != sc.IntervalSeconds
		if sc.Enabled && changed {
			if sc.CronExpr == "" && sc.IntervalSeconds > 0 {
				s.lastRun[sc.ID] = now
			} else {
				delete(s.lastRun, sc.ID)
			}
		}
		kept = append(kept, sc)
	}
	// Drop bookkeeping for schedules that no longer exist (rule removed or
	// workflow deleted) so lastRun cannot grow without bound.
	for id := range old {
		if !registered[id] {
			delete(s.lastRun, id)
		}
	}
	s.schedules = kept
}

// Start blocks until ctx is cancelled. It wakes at the top of every
// minute (a single timer for every schedule, not one goroutine/timer per
// schedule, so the scheduler's own footprint stays flat regardless of how
// many Schedule Trigger nodes exist -- rule 20: no unnecessary workers).
// Waking on the minute boundary rather than on a free-running 1-minute
// ticker matters: a ticker started at, say, 10:00:59.99 can land a
// millisecond either side of a minute edge, which made cron minutes get
// skipped or hit twice and made "every N minutes" intervals slip to N+1.
// One-minute resolution is inherent: an interval under 60s fires at most
// once a minute.
func (s *Scheduler) Start(ctx context.Context) {
	s.tick(ctx, time.Now()) // catch a cron minute that is already current at startup
	for {
		now := time.Now()
		// A small slack past the boundary keeps a wall-clock adjustment from
		// making the woken tick still read as the previous minute.
		next := now.Truncate(time.Minute).Add(time.Minute + 250*time.Millisecond)
		timer := time.NewTimer(next.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case t := <-timer.C:
			s.tick(ctx, t)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	minute := now.Truncate(time.Minute)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sc := range s.schedules {
		if !sc.Enabled {
			continue
		}
		last, ran := s.lastRun[sc.ID]
		due := false
		switch {
		case sc.CronExpr != "":
			// Never fire the same schedule twice for one wall-clock minute
			// (e.g. the startup tick followed by the first boundary tick).
			due = cronMatches(sc.CronExpr, minute) && !(ran && !last.Before(minute))
		case sc.IntervalSeconds > 0:
			due = !ran || now.Sub(last) >= time.Duration(sc.IntervalSeconds)*time.Second
		}
		if due {
			// Stored on the minute boundary so a fixed interval stays exact
			// instead of accumulating the tick's sub-second offset.
			s.lastRun[sc.ID] = minute

			// Overlap protection is per Schedule Trigger node, not per rule:
			// a node with several rules must still never run twice at once.
			key := sc.WorkflowID + "/" + sc.NodeName
			s.inFlightMu.Lock()
			if s.inFlight[key] {
				s.inFlightMu.Unlock()
				log.Printf("scheduler: %s/%s still running from a previous tick, skipping this one", sc.WorkflowID, sc.NodeName)
				continue
			}
			s.inFlight[key] = true
			s.inFlightMu.Unlock()

			go func(sc Schedule, key string) {
				defer func() {
					s.inFlightMu.Lock()
					delete(s.inFlight, key)
					s.inFlightMu.Unlock()
					if r := recover(); r != nil {
						log.Printf("scheduler: run for %s/%s panicked: %v", sc.WorkflowID, sc.NodeName, r)
					}
				}()
				s.run(ctx, sc.WorkflowID, sc.NodeName)
			}(sc, key)
		}
	}
}

// cronMatches implements standard 5-field cron (minute hour day month
// weekday). Each field accepts '*', a number, an 'a-b' range, a '*/n' or
// 'a-b/n' step, and comma lists of those; weekday 7 is Sunday like 0. As
// in standard cron, when both day-of-month and weekday are restricted a
// date matches if EITHER does. Deliberately not a full cron library to
// avoid an extra dependency for a small feature (rule 20: no unnecessary
// deps in a RAM-constrained build).
func cronMatches(expr string, t time.Time) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	if !fieldMatches(fields[0], t.Minute(), 0, 59) ||
		!fieldMatches(fields[1], t.Hour(), 0, 23) ||
		!fieldMatches(fields[3], int(t.Month()), 1, 12) {
		return false
	}
	dom := fieldMatches(fields[2], t.Day(), 1, 31)
	wd := int(t.Weekday())
	dow := fieldMatches(fields[4], wd, 0, 7) || (wd == 0 && fieldMatches(fields[4], 7, 0, 7))
	if !strings.HasPrefix(fields[2], "*") && !strings.HasPrefix(fields[4], "*") {
		return dom || dow
	}
	return dom && dow
}

// CronValid reports whether expr is a 5-field cron expression that
// cronMatches can evaluate, so a malformed one can be reported at
// registration instead of silently never firing.
func CronValid(expr string) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	limits := [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	for i, f := range fields {
		for _, part := range strings.Split(f, ",") {
			if _, _, _, ok := parseCronPart(part, limits[i][0], limits[i][1]); !ok {
				return false
			}
		}
	}
	return true
}

func fieldMatches(field string, value, min, max int) bool {
	for _, part := range strings.Split(field, ",") {
		lo, hi, step, ok := parseCronPart(part, min, max)
		if ok && value >= lo && value <= hi && (value-lo)%step == 0 {
			return true
		}
	}
	return false
}

// parseCronPart parses one comma-separated element of a cron field
// ("*", "5", "1-4", "*/15", "0-30/10") into an inclusive [lo,hi] range
// and a step, rejecting anything outside [min,max].
func parseCronPart(part string, min, max int) (lo, hi, step int, ok bool) {
	step = 1
	if i := strings.IndexByte(part, '/'); i >= 0 {
		n, err := strconv.Atoi(part[i+1:])
		if err != nil || n <= 0 {
			return 0, 0, 0, false
		}
		step = n
		part = part[:i]
	}
	switch {
	case part == "*":
		lo, hi = min, max
	case strings.Contains(part, "-"):
		i := strings.IndexByte(part, '-')
		a, err1 := strconv.Atoi(part[:i])
		b, err2 := strconv.Atoi(part[i+1:])
		if err1 != nil || err2 != nil {
			return 0, 0, 0, false
		}
		lo, hi = a, b
	default:
		a, err := strconv.Atoi(part)
		if err != nil {
			return 0, 0, 0, false
		}
		lo, hi = a, a
		if step > 1 {
			hi = max
		}
	}
	if lo < min || hi > max || lo > hi {
		return 0, 0, 0, false
	}
	return lo, hi, step, true
}

func (s *Schedule) String() string {
	return fmt.Sprintf("%s/%s", s.WorkflowID, s.NodeName)
}
