// Handlers backing the "Run All Services" Schedule feature: you set a
// time/day (cron or a simple interval), and when it's due the existing
// global scheduler (internal/scheduler) fires Run All Services through
// the exact same runall.Manager.StartAll path the "Run All Services"
// button uses -- every Service then runs exactly as it does today,
// strictly sequential, through the one existing queue (rule: no new
// runner/queue). Registered by WithRunAllSchedules so a Server built
// without it (every pre-existing test helper) exposes none of these
// routes, matching WithTenancy/WithAsync's nil-safe pattern elsewhere
// in this package.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"microflow/internal/scheduler"
	"microflow/internal/store"
)

func (s *Server) routesRunAllSchedules() {
	s.mux.HandleFunc("GET /api/run-all-schedules", s.handleListRunAllSchedules)
	s.mux.HandleFunc("POST /api/run-all-schedules", s.handleCreateRunAllSchedule)
	s.mux.HandleFunc("GET /api/run-all-schedules/{id}", s.handleGetRunAllSchedule)
	s.mux.HandleFunc("PATCH /api/run-all-schedules/{id}", s.handleUpdateRunAllSchedule)
	s.mux.HandleFunc("DELETE /api/run-all-schedules/{id}", s.handleDeleteRunAllSchedule)
	s.mux.HandleFunc("POST /api/run-all-schedules/{id}/enable", s.handleEnableRunAllSchedule)
	s.mux.HandleFunc("POST /api/run-all-schedules/{id}/disable", s.handleDisableRunAllSchedule)
}

// runAllScheduleRequest is the Add/Edit request body: cron ("minute
// hour day month weekday") or a plain interval in seconds -- exactly
// one of the two, mirroring a Schedule Trigger node's own two shapes.
type runAllScheduleRequest struct {
	Label           string `json:"label"`
	CronExpr        string `json:"cronExpr"`
	IntervalSeconds int    `json:"intervalSeconds"`
	StopOnFailure   bool   `json:"stopOnFailure"`
	// Optional: Start/End as wall-clock "2006-01-02T15:04" in the
	// scheduler's timezone; MaxRuns 0 = Unlimited.
	StartAt string `json:"startAt"`
	EndAt   string `json:"endAt"`
	MaxRuns int    `json:"maxRuns"`
}

const rasTimeLayout = "2006-01-02T15:04"

func (s *Server) schedulerLoc() *time.Location {
	if s.schedLoc != nil {
		return s.schedLoc
	}
	return time.UTC
}

func parseRasTime(v string, loc *time.Location) (*time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, nil
	}
	for _, layout := range []string{rasTimeLayout, "2006-01-02T15:04:05"} {
		if t, err := time.ParseInLocation(layout, v, loc); err == nil {
			return &t, nil
		}
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t, nil
	}
	return nil, errors.New("invalid start/end time -- use YYYY-MM-DDTHH:MM")
}

// window validates and parses the optional Start/End/Repeat fields.
func (req *runAllScheduleRequest) window(loc *time.Location) (start, end *time.Time, maxRuns int, err error) {
	if req.MaxRuns < 0 {
		return nil, nil, 0, errors.New("repeat count cannot be negative (use 0 for unlimited)")
	}
	if start, err = parseRasTime(req.StartAt, loc); err != nil {
		return nil, nil, 0, err
	}
	if end, err = parseRasTime(req.EndAt, loc); err != nil {
		return nil, nil, 0, err
	}
	if start != nil && end != nil && !end.After(*start) {
		return nil, nil, 0, errors.New("end time must be after start time")
	}
	return start, end, req.MaxRuns, nil
}

func (req *runAllScheduleRequest) normalize() (cronExpr string, intervalSeconds int, err error) {
	cronExpr = strings.TrimSpace(req.CronExpr)
	intervalSeconds = req.IntervalSeconds
	switch {
	case cronExpr != "" && intervalSeconds > 0:
		return "", 0, errors.New("set either cronExpr or intervalSeconds, not both")
	case cronExpr != "":
		if !scheduler.CronValid(cronExpr) {
			return "", 0, errors.New("invalid cron expression -- need 5 fields: minute hour day month weekday")
		}
		return cronExpr, 0, nil
	case intervalSeconds > 0:
		return "", intervalSeconds, nil
	default:
		return "", 0, errors.New("set either cronExpr or intervalSeconds")
	}
}

func decodeRunAllScheduleRequest(r *http.Request) (*runAllScheduleRequest, error) {
	limited := io.LimitReader(r.Body, 8192)
	var req runAllScheduleRequest
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		return nil, errors.New("invalid request body")
	}
	return &req, nil
}

func runAllScheduleResponse(row store.RunAllScheduleRow, loc *time.Location) map[string]any {
	fmtT := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.In(loc).Format(rasTimeLayout)
	}
	return map[string]any{
		"id":              row.ID,
		"label":           row.Label,
		"cronExpr":        row.CronExpr,
		"intervalSeconds": row.IntervalSeconds,
		"stopOnFailure":   row.StopOnFailure,
		"enabled":         row.Enabled,
		"startAt":         fmtT(row.StartAt),
		"endAt":           fmtT(row.EndAt),
		"maxRuns":         row.MaxRuns,
		"runCount":        row.RunCount,
		"createdAt":       row.CreatedAt,
		"updatedAt":       row.UpdatedAt,
	}
}

// syncRunAllSchedules re-lists every schedule from the store and pushes
// the full set into the running scheduler, so a create/edit/delete/
// enable/disable takes effect immediately without a restart. Errors are
// swallowed on purpose (same as onWorkflowChanged elsewhere): the HTTP
// response about the just-made change should not fail just because the
// best-effort re-sync's own re-list query hiccupped -- the next
// mutation (or a restart, which always re-Loads from the store) will
// reconcile it.
func (s *Server) syncRunAllSchedules(r *http.Request) {
	if s.onRunAllSchedulesChanged == nil {
		return
	}
	rows, err := s.runAllSchedules.ListRunAllSchedules(r.Context())
	if err != nil {
		return
	}
	s.onRunAllSchedulesChanged(rows)
}

func (s *Server) handleListRunAllSchedules(w http.ResponseWriter, r *http.Request) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	rows, err := s.runAllSchedules.ListRunAllSchedules(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list run-all schedules"))
		return
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		out[i] = runAllScheduleResponse(row, s.schedulerLoc())
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	row, err := s.runAllSchedules.GetRunAllSchedule(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrRunAllScheduleNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("run-all schedule not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to load run-all schedule"))
		return
	}
	writeJSON(w, http.StatusOK, runAllScheduleResponse(*row, s.schedulerLoc()))
}

// handleCreateRunAllSchedule adds a schedule. It always starts Disabled
// (enabled=false) regardless of the request body -- a newly added
// schedule must never start firing on its own; a person enables it
// explicitly (POST .../enable) once they're happy with it.
func (s *Server) handleCreateRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	req, err := decodeRunAllScheduleRequest(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cronExpr, intervalSeconds, err := req.normalize()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	startAt, endAt, maxRuns, err := req.window(s.schedulerLoc())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	row := store.RunAllScheduleRow{
		StartAt:         startAt,
		EndAt:           endAt,
		MaxRuns:         maxRuns,
		ID:              "ras-" + randSuffix() + randSuffix(),
		Label:           strings.TrimSpace(req.Label),
		CronExpr:        cronExpr,
		IntervalSeconds: intervalSeconds,
		StopOnFailure:   req.StopOnFailure,
		Enabled:         false,
	}
	if err := s.runAllSchedules.CreateRunAllSchedule(r.Context(), row); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to create run-all schedule"))
		return
	}
	s.syncRunAllSchedules(r)
	writeJSON(w, http.StatusCreated, runAllScheduleResponse(row, s.schedulerLoc()))
}

// handleUpdateRunAllSchedule edits label/timing/stop-on-failure.
// Enabled state is deliberately untouched here -- use the dedicated
// enable/disable endpoints so an edit never silently flips it.
func (s *Server) handleUpdateRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	id := r.PathValue("id")
	existing, err := s.runAllSchedules.GetRunAllSchedule(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrRunAllScheduleNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("run-all schedule not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to load run-all schedule"))
		return
	}
	req, err := decodeRunAllScheduleRequest(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cronExpr, intervalSeconds, err := req.normalize()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	startAt, endAt, maxRuns, err := req.window(s.schedulerLoc())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if maxRuns != existing.MaxRuns {
		existing.RunCount = 0 // a new Repeat limit starts counting afresh
	}
	existing.StartAt, existing.EndAt, existing.MaxRuns = startAt, endAt, maxRuns
	existing.Label = strings.TrimSpace(req.Label)
	existing.CronExpr = cronExpr
	existing.IntervalSeconds = intervalSeconds
	existing.StopOnFailure = req.StopOnFailure
	if err := s.runAllSchedules.UpdateRunAllSchedule(r.Context(), *existing); err != nil {
		if errors.Is(err, store.ErrRunAllScheduleNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("run-all schedule not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to update run-all schedule"))
		return
	}
	s.syncRunAllSchedules(r)
	writeJSON(w, http.StatusOK, runAllScheduleResponse(*existing, s.schedulerLoc()))
}

func (s *Server) handleDeleteRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	id := r.PathValue("id")
	if err := s.runAllSchedules.DeleteRunAllSchedule(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrRunAllScheduleNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("run-all schedule not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete run-all schedule"))
		return
	}
	s.syncRunAllSchedules(r)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) setRunAllScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if s.runAllSchedules == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all schedules are not enabled on this server"))
		return
	}
	id := r.PathValue("id")
	existing, err := s.runAllSchedules.GetRunAllSchedule(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrRunAllScheduleNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("run-all schedule not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to load run-all schedule"))
		return
	}
	existing.Enabled = enabled
	if enabled && existing.MaxRuns > 0 && existing.RunCount >= existing.MaxRuns {
		existing.RunCount = 0 // re-enabling a finished Repeat schedule starts it over
	}
	if err := s.runAllSchedules.UpdateRunAllSchedule(r.Context(), *existing); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to update run-all schedule"))
		return
	}
	s.syncRunAllSchedules(r)
	writeJSON(w, http.StatusOK, runAllScheduleResponse(*existing, s.schedulerLoc()))
}

func (s *Server) handleEnableRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	s.setRunAllScheduleEnabled(w, r, true)
}

func (s *Server) handleDisableRunAllSchedule(w http.ResponseWriter, r *http.Request) {
	s.setRunAllScheduleEnabled(w, r, false)
}

// handleScheduleNextRuns lists the next run of every registered schedule
// (workflow Schedule Triggers and Run All Services schedules). Times are
// shown in the scheduler's timezone, the one cron fields are read in.
func (s *Server) handleScheduleNextRuns(w http.ResponseWriter, r *http.Request) {
	loc := s.schedulerLoc()
	now := time.Now()
	items := []map[string]any{}
	for _, info := range s.nextRuns(now) {
		item := map[string]any{
			"id":          info.ID,
			"workflowId":  info.WorkflowID,
			"nodeName":    info.NodeName,
			"runAll":      info.RunAll,
			"enabled":     info.Enabled,
			"state":       info.State,
			"nextRun":     nil,
			"nextRunText": "",
		}
		if !info.Next.IsZero() {
			item["nextRun"] = info.Next.Format(time.RFC3339)
			item["nextRunText"] = info.Next.In(loc).Format("2006-01-02 15:04")
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"timezone":  loc.String(),
		"now":       now.Format(time.RFC3339),
		"schedules": items,
	})
}
