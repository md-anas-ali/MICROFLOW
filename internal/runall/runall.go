// Package runall implements "Run Service" and "Run All Services": a
// durable, strictly sequential sweep across one or every MicroFlow
// Service's workflows (rule 8 -- "parallel execution করবে না... Strict
// sequential execution হবে"). Progress is persisted after every single
// workflow step (via store.RunAllJobRow), so a server restart mid-sweep
// resumes from the last recorded step instead of losing the queue or
// re-running already-completed work (rule 17: durable + idempotent).
//
// This deliberately does NOT duplicate runner.Runner's execution logic:
// every workflow step goes through the exact same RunFromNode path a
// manual "Run" click uses, so scratch-dir isolation, checkpointing,
// execution persistence, and cleanup are all the one existing
// implementation (rule: no duplicate runner/execution engine).
package runall

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"microflow/internal/model"
	"microflow/internal/runner"
	"microflow/internal/scheduler"
	"microflow/internal/store"
	"microflow/internal/tenant"
)

// ErrAlreadyActive is returned by StartAll/StartOne when a sweep is
// already queued or running (rule: "Duplicate Run All execution
// prevent করো") -- the API maps this to HTTP 409.
var ErrAlreadyActive = errors.New("runall: a Run All Services sweep is already in progress")

// StepResult records the outcome of running one workflow as part of a
// sweep -- one entry per workflow, appended and persisted as the sweep
// progresses, so recovery after a restart knows exactly what's already
// done.
type StepResult struct {
	ServiceID    string `json:"serviceId"`
	ServiceName  string `json:"serviceName,omitempty"`
	WorkflowID   string `json:"workflowId"`
	WorkflowName string `json:"workflowName"`
	ExecutionID  string `json:"executionId,omitempty"`
	Status       string `json:"status"` // success | error | skipped
	Error        string `json:"error,omitempty"`
}

// Status is the sweep state the dashboard polls.
type Status struct {
	ID         string       `json:"id,omitempty"`
	Status     string       `json:"status"` // idle | queued | running | success | error | cancelled
	ServiceIDs []string     `json:"serviceIds,omitempty"`
	Results    []StepResult `json:"results"`
	Error      string       `json:"error,omitempty"`
	StartedAt  time.Time    `json:"startedAt,omitempty"`
	UpdatedAt  time.Time    `json:"updatedAt,omitempty"`
	FinishedAt *time.Time   `json:"finishedAt,omitempty"`
}

// Store is the persistence subset runall needs; *store.Store satisfies it.
type Store interface {
	CreateRunAllJob(ctx context.Context, id string, serviceIDs []string, stopOnFailure bool) error
	UpdateRunAllJob(ctx context.Context, id, status string, results json.RawMessage, jobErr string) error
	GetActiveRunAllJob(ctx context.Context) (*store.RunAllJobRow, error)
	ListRunAllJobs(ctx context.Context, limit int) ([]*store.RunAllJobRow, error)
	ListServices(ctx context.Context) ([]*tenant.Service, error)
	ListWorkflowsByService(ctx context.Context, serviceID string) ([]*model.Workflow, error)
}

// WorkflowRunner is the execution subset runall needs; *runner.Runner
// satisfies it -- every step runs through the identical path a manual
// "Run" click uses (see package doc).
type WorkflowRunner interface {
	RunFromNode(ctx context.Context, workflowID, startNode, mode string, seed model.NodeOutput) (*model.Execution, error)
}

// Manager coordinates one sweep at a time. The in-memory `active` flag
// is a fast pre-check; the schema's partial unique index on
// run_all_jobs is the actual durable guard against a race (two
// concurrent server processes, or a restart landing mid-request).
type Manager struct {
	store Store
	run   WorkflowRunner

	mu       sync.Mutex
	active   bool
	cancelFn context.CancelFunc
}

func NewManager(st Store, run WorkflowRunner) *Manager {
	return &Manager{store: st, run: run}
}

// Resume is called once at server startup: if a sweep was left
// queued/running when the process last stopped, it picks back up from
// the last persisted step instead of losing the queue (rule 17).
func (m *Manager) Resume(ctx context.Context) {
	row, err := m.store.GetActiveRunAllJob(ctx)
	if err != nil {
		log.Printf("runall: resume check failed: %v", err)
		return
	}
	if row == nil {
		return
	}
	var resume []StepResult
	if len(row.Results) > 0 {
		if err := json.Unmarshal(row.Results, &resume); err != nil {
			log.Printf("runall: resume: could not decode prior results for job %s: %v", row.ID, err)
		}
	}
	log.Printf("runall: resuming sweep %s (%d service(s), %d step(s) already recorded)", row.ID, len(row.ServiceIDs), len(resume))
	m.mu.Lock()
	m.active = true
	runCtx, cancel := context.WithCancel(context.Background())
	m.cancelFn = cancel
	m.mu.Unlock()
	go m.runSweep(runCtx, row.ID, row.ServiceIDs, row.StopOnFailure, resume)
}

// StartAll begins a sweep across every Service, in their stable
// (creation-order) listing order.
func (m *Manager) StartAll(ctx context.Context, stopOnFailure bool) (string, error) {
	services, err := m.store.ListServices(ctx)
	if err != nil {
		return "", err
	}
	ids := make([]string, len(services))
	for i, svc := range services {
		ids[i] = svc.ID
	}
	if len(ids) == 0 {
		return "", errors.New("runall: no services exist yet")
	}
	return m.start(ctx, ids, stopOnFailure)
}

// StartOne begins a "sweep" over just one Service -- this is what "Run
// Service" uses, so a single Service's multiple workflows (if it has
// more than one) still run strictly sequentially and durably, exactly
// like Run All Services does across Services.
func (m *Manager) StartOne(ctx context.Context, serviceID string, stopOnFailure bool) (string, error) {
	return m.start(ctx, []string{serviceID}, stopOnFailure)
}

func (m *Manager) start(ctx context.Context, serviceIDs []string, stopOnFailure bool) (string, error) {
	m.mu.Lock()
	if m.active {
		m.mu.Unlock()
		return "", ErrAlreadyActive
	}
	m.active = true
	m.mu.Unlock()

	id := newID()
	if err := m.store.CreateRunAllJob(ctx, id, serviceIDs, stopOnFailure); err != nil {
		m.mu.Lock()
		m.active = false
		m.mu.Unlock()
		if errors.Is(err, store.ErrRunAllAlreadyActive) {
			return "", ErrAlreadyActive
		}
		return "", err
	}

	// The sweep must outlive the HTTP request that started it (it can
	// legitimately run for a long time -- rule 8), so it runs on a
	// detached context, cancellable only via Cancel() below, not by the
	// starting request's own context being cancelled/timing out.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.mu.Lock()
	m.cancelFn = cancel
	m.mu.Unlock()
	go m.runSweep(runCtx, id, serviceIDs, stopOnFailure, nil)
	return id, nil
}

// Cancel requests the in-progress sweep stop. Already-running workflow
// steps are allowed to finish (or themselves respond to the cancelled
// context, same as any other execution); no further service/workflow
// in the sweep is started once cancellation is observed.
func (m *Manager) Cancel(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active || m.cancelFn == nil {
		return errors.New("runall: no sweep is currently in progress")
	}
	m.cancelFn()
	return nil
}

// Status reports the current or, if idle, most recently finished sweep.
func (m *Manager) Status(ctx context.Context) (*Status, error) {
	row, err := m.store.GetActiveRunAllJob(ctx)
	if err != nil {
		return nil, err
	}
	if row == nil {
		jobs, err := m.store.ListRunAllJobs(ctx, 1)
		if err != nil {
			return nil, err
		}
		if len(jobs) == 0 {
			return &Status{Status: "idle", Results: []StepResult{}}, nil
		}
		return rowToStatus(jobs[0])
	}
	return rowToStatus(row)
}

func rowToStatus(row *store.RunAllJobRow) (*Status, error) {
	st := &Status{
		ID:         row.ID,
		Status:     row.Status,
		ServiceIDs: row.ServiceIDs,
		Error:      row.Error,
		StartedAt:  row.StartedAt,
		UpdatedAt:  row.UpdatedAt,
		FinishedAt: row.FinishedAt,
		Results:    []StepResult{},
	}
	if len(row.Results) > 0 {
		if err := json.Unmarshal(row.Results, &st.Results); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// runSweep is the actual sequential loop: for each Service, in order,
// run every one of its workflows to completion (via the shared Runner,
// which already handles scratch-dir isolation and per-execution
// cleanup -- rule 9 is satisfied by that existing mechanism, nothing
// extra is needed here) before moving to the next workflow, and only
// move to the next Service once the current one's workflows have all
// finished. Persists `results` after every step so a restart resumes
// correctly (see Resume).
func (m *Manager) runSweep(ctx context.Context, jobID string, serviceIDs []string, stopOnFailure bool, resume []StepResult) {
	defer func() {
		m.mu.Lock()
		m.active = false
		m.cancelFn = nil
		m.mu.Unlock()
	}()

	results := append([]StepResult{}, resume...)
	done := make(map[string]bool, len(resume))
	for _, r := range resume {
		done[r.ServiceID+"/"+r.WorkflowID] = true
	}

	persist := func(status, jobErr string) {
		raw, err := json.Marshal(results)
		if err != nil {
			log.Printf("runall: encode results for job %s failed: %v", jobID, err)
			return
		}
		if err := m.store.UpdateRunAllJob(context.Background(), jobID, status, raw, jobErr); err != nil {
			log.Printf("runall: persist progress for job %s failed: %v", jobID, err)
		}
	}

	finalStatus := "success"
	var jobErr string

outer:
	for _, svcID := range serviceIDs {
		if ctx.Err() != nil {
			finalStatus = "cancelled"
			break
		}
		svc, svcErr := m.serviceName(ctx, svcID)
		workflows, err := m.store.ListWorkflowsByService(ctx, svcID)
		if err != nil {
			results = append(results, StepResult{ServiceID: svcID, ServiceName: svc, Status: "error", Error: err.Error()})
			finalStatus, jobErr = "error", err.Error()
			persist("running", jobErr)
			if stopOnFailure {
				break outer
			}
			continue
		}
		_ = svcErr

		for _, wf := range workflows {
			if ctx.Err() != nil {
				finalStatus = "cancelled"
				break outer
			}
			key := svcID + "/" + wf.ID
			if done[key] {
				continue // already recorded in a prior (pre-restart) pass
			}
			start := runner.FirstTriggerNode(wf)
			step := StepResult{ServiceID: svcID, ServiceName: svc, WorkflowID: wf.ID, WorkflowName: wf.Name}
			if start == "" {
				step.Status = "skipped"
				step.Error = "no trigger node found"
				results = append(results, step)
				persist("running", jobErr)
				continue
			}
			seed := model.NodeOutput{{{JSON: map[string]any{}}}}
			ex, runErr := m.run.RunFromNode(ctx, wf.ID, start, "run-all", seed)
			if ex != nil {
				step.ExecutionID = ex.ID
			}
			switch {
			case runErr != nil && (errors.Is(runErr, scheduler.ErrDuplicate) || errors.Is(runErr, runner.ErrAlreadyQueued)):
				// The workflow is already queued or running -- typically
				// resumed by crash recovery after a restart, or started by
				// a manual Run/webhook. That run is already doing this
				// Service's work, and starting it again would only create a
				// duplicate execution. This is not a failure of the Service,
				// so record it as skipped instead of marking the whole
				// sweep as "error". The scheduler's single FIFO queue still
				// keeps the next Service waiting behind the run in flight.
				step.Status = "skipped"
				step.Error = "already queued or running (e.g. resumed after a restart) -- not started again to avoid a duplicate run"
			case runErr != nil:
				step.Status, step.Error = "error", runErr.Error()
			case ex != nil && ex.Status == model.StatusError:
				step.Status, step.Error = "error", ex.Error
			case ex != nil && ex.Status == model.StatusCancelled:
				step.Status, step.Error = "error", "execution cancelled"
			default:
				step.Status = "success"
			}
			results = append(results, step)
			if step.Status == "error" {
				finalStatus, jobErr = "error", step.Error
			}
			persist("running", jobErr)
			if step.Status == "error" && stopOnFailure {
				break outer
			}
		}
		// Per-execution scratch/DB cleanup already happened inside
		// RunFromNode for every step above (runner.runOnce's existing
		// cleanupScratch + immediate DeleteExecution) -- nothing
		// additional to clean up per-Service (rule 9 is already covered
		// by the existing mechanism, reused here rather than duplicated).
	}

	if ctx.Err() != nil {
		finalStatus = "cancelled"
	}
	persist(finalStatus, jobErr)
	log.Printf("runall: sweep %s finished: status=%s services=%d steps=%d", jobID, finalStatus, len(serviceIDs), len(results))
}

func (m *Manager) serviceName(ctx context.Context, id string) (string, error) {
	services, err := m.store.ListServices(ctx)
	if err != nil {
		return "", err
	}
	for _, s := range services {
		if s.ID == id {
			return s.Name, nil
		}
	}
	return "", fmt.Errorf("runall: service %q not found", id)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "runall-" + hex.EncodeToString(b)
}
