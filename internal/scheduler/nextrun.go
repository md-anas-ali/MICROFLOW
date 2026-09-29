package scheduler

import (
	"strings"
	"time"
)

// NextRunInfo describes when one registered schedule (a workflow's
// Schedule Trigger rule or a Run All Services schedule) fires next.
type NextRunInfo struct {
	ID         string
	WorkflowID string
	NodeName   string
	RunAll     bool
	Enabled    bool
	Next       time.Time // zero when there is no upcoming run
	// State: scheduled | disabled | ended | completed | invalid
	State string
}

func ceilMinute(t time.Time) time.Time {
	tr := t.Truncate(time.Minute)
	if tr.Equal(t) {
		return t
	}
	return tr.Add(time.Minute)
}

// NextRuns reports the next fire time of every registered schedule as
// of now. Read-only: it never changes scheduling state.
func (s *Scheduler) NextRuns(now time.Time) []NextRunInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]NextRunInfo, 0, len(s.schedules))
	for _, sc := range s.schedules {
		info := NextRunInfo{ID: sc.ID, WorkflowID: sc.WorkflowID, NodeName: sc.NodeName, RunAll: sc.RunAll, Enabled: sc.Enabled}
		switch {
		case sc.RunAll && sc.MaxRuns > 0 && sc.RunCount >= sc.MaxRuns:
			info.State = "completed"
		case !sc.Enabled:
			info.State = "disabled"
		default:
			next := s.nextFor(sc, now)
			switch {
			case !next.IsZero():
				info.Next, info.State = next, "scheduled"
			case sc.RunAll && !sc.EndAt.IsZero():
				info.State = "ended"
			default:
				info.State = "invalid"
			}
		}
		out = append(out, info)
	}
	return out
}

// nextFor computes the next tick-aligned fire time (zero = none). Caller
// holds s.mu.
func (s *Scheduler) nextFor(sc Schedule, now time.Time) time.Time {
	lb := now.Truncate(time.Minute).Add(time.Minute) // earliest tick still ahead
	if sc.RunAll && !sc.StartAt.IsZero() {
		if st := ceilMinute(sc.StartAt); st.After(lb) {
			lb = st
		}
	}
	var next time.Time
	switch {
	case sc.CronExpr != "":
		t, ok := nextCronTime(sc.CronExpr, lb.In(s.location))
		if !ok {
			return time.Time{}
		}
		next = t
	case sc.IntervalSeconds > 0:
		next = lb
		if last, ran := s.lastRun[sc.ID]; ran {
			if t := ceilMinute(last.Add(time.Duration(sc.IntervalSeconds) * time.Second)); t.After(lb) {
				next = t
			}
		}
	default:
		return time.Time{}
	}
	if sc.RunAll && !sc.EndAt.IsZero() && next.After(sc.EndAt) {
		return time.Time{}
	}
	return next
}

// nextCronTime returns the first minute >= from (already minute-aligned,
// in the zone the fields are read in) matching the 5-field cron
// expression, searching up to 5 years ahead. It skips whole days/hours
// that cannot match instead of stepping minute by minute, and uses the
// same field semantics as cronMatches (including dom/weekday OR rule).
func nextCronTime(expr string, from time.Time) (time.Time, bool) {
	f := strings.Fields(expr)
	if len(f) != 5 {
		return time.Time{}, false
	}
	t := from.Truncate(time.Minute)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		if !cronDayMatches(f, t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !fieldMatches(f[1], t.Hour(), 0, 23) {
			n := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			if !n.After(t) {
				n = t.Add(time.Hour)
			}
			t = n
			continue
		}
		if fieldMatches(f[0], t.Minute(), 0, 59) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

func cronDayMatches(f []string, t time.Time) bool {
	if !fieldMatches(f[3], int(t.Month()), 1, 12) {
		return false
	}
	dom := fieldMatches(f[2], t.Day(), 1, 31)
	wd := int(t.Weekday())
	dow := fieldMatches(f[4], wd, 0, 7) || (wd == 0 && fieldMatches(f[4], 7, 0, 7))
	if !strings.HasPrefix(f[2], "*") && !strings.HasPrefix(f[4], "*") {
		return dom || dow
	}
	return dom && dow
}
