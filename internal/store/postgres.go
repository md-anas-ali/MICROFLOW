// Package store is MicroFlow's Postgres persistence layer. It works
// against ANY standard PostgreSQL reachable via DATABASE_URL (Neon,
// Supabase, Render, Railway, self-hosted) — nothing here is
// provider-specific. Uses pgx's connection pool kept intentionally
// small for the 512MB RAM budget (rule 12/18).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"microflow/internal/model"
	"microflow/internal/tenant"
)

type Store struct {
	pool *pgxpool.Pool
}

// Open connects using DATABASE_URL and a small pool (default max 2
// connections -- this deployment only ever runs one workflow with
// MaxConcurrentExecutions=1, so one connection for the run itself plus
// one spare for the periodic history-cleanup/API-status queries is
// already generous; override via MICROFLOW_DB_MAX_CONNS if needed).
// Each pgx connection holds its own read/write buffers, so on a ~170MB
// total budget this is a real, if small, RAM line item -- not just a
// connection-count knob.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: invalid DATABASE_URL: %w", err)
	}
	maxConns := int32(2)
	if v := os.Getenv("MICROFLOW_DB_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxConns = int32(n)
		}
	}
	cfg.MaxConns = maxConns
	// MinConns defaults to 0 (pgxpool opens connections lazily), which we
	// keep -- an idle server shouldn't hold any DB connections open at
	// all. MaxConnIdleTime/MaxConnLifetime release connections back
	// (closing the socket + freeing pgx's per-conn buffers) once a burst
	// of activity (e.g. a batch of scheduled runs) is over, instead of
	// keeping up to maxConns connections idle-but-allocated indefinitely
	// (rule: release idle resources).
	cfg.MaxConnIdleTime = 3 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// ApplySchema runs schema.sql. Idempotent (CREATE TABLE IF NOT EXISTS).
func (s *Store) ApplySchema(ctx context.Context, schemaSQL string) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// --- services (tenants -- see internal/tenant) ---

// ErrServiceNotFound is returned by RenameService/DeleteService/
// GetService when no Service with the given id exists (mapped to 404).
var ErrServiceNotFound = errors.New("store: service not found")

// ErrServiceHasActiveExecutions is returned by DeleteService when any
// workflow belonging to the Service still has a queued/running/waiting
// execution -- mirrors ErrWorkflowHasActiveExecutions's protection but
// checked across every workflow in the Service at once (rule: protect
// against accidental destructive action).
var ErrServiceHasActiveExecutions = errors.New("store: service has a running or queued execution and cannot be deleted")

// CreateService inserts a new Service. id must already be a generated,
// unique, URL/path-safe identifier (the API layer generates it).
func (s *Store) CreateService(ctx context.Context, svc *tenant.Service) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO services (id, name, created_at, updated_at) VALUES ($1, $2, now(), now())
	`, svc.ID, svc.Name)
	return err
}

func (s *Store) GetService(ctx context.Context, id string) (*tenant.Service, error) {
	var svc tenant.Service
	err := s.pool.QueryRow(ctx, `SELECT id, name, created_at, updated_at FROM services WHERE id=$1`, id).
		Scan(&svc.ID, &svc.Name, &svc.CreatedAt, &svc.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrServiceNotFound
		}
		return nil, err
	}
	return &svc, nil
}

// ListServices returns every Service, oldest first (stable, predictable
// order for both the dashboard list and Run All Services' default order).
func (s *Store) ListServices(ctx context.Context) ([]*tenant.Service, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, created_at, updated_at FROM services ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*tenant.Service{}
	for rows.Next() {
		var svc tenant.Service
		if err := rows.Scan(&svc.ID, &svc.Name, &svc.CreatedAt, &svc.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &svc)
	}
	return out, rows.Err()
}

func (s *Store) RenameService(ctx context.Context, id, name string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE services SET name=$2, updated_at=now() WHERE id=$1`, id, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrServiceNotFound
	}
	return nil
}

// DeleteService removes a Service and, via ON DELETE CASCADE, every
// workflow/credential/execution/schedule/env-override scoped to it
// (rule 4: complete isolation cuts both ways -- deleting a Service
// deletes everything inside it, nothing outside it). Guarded the same
// way DeleteWorkflow is guarded: the Service row is locked first, then
// every workflow belonging to it is checked for in-flight executions
// inside the same transaction, so a run cannot start in the gap
// between the check and the DELETE.
func (s *Store) DeleteService(ctx context.Context, id string) error {
	if id == tenant.DefaultID {
		return errors.New("store: the Default service cannot be deleted")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, `SELECT true FROM services WHERE id=$1 FOR UPDATE`, id).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrServiceNotFound
		}
		return err
	}

	var activeCount int
	err = tx.QueryRow(ctx, `
		SELECT count(*) FROM executions e
		JOIN workflows w ON w.id = e.workflow_id
		WHERE w.service_id = $1 AND e.finished_at IS NULL
		  AND e.status IN ('queued', 'running', 'waiting')
	`, id).Scan(&activeCount)
	if err != nil {
		return err
	}
	if activeCount > 0 {
		return ErrServiceHasActiveExecutions
	}

	if _, err := tx.Exec(ctx, `DELETE FROM services WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CountWorkflowsInService is a cheap pre-delete check the API surfaces
// to the person before they confirm deleting a Service (rule: protect
// against accidental destructive action).
func (s *Store) CountWorkflowsInService(ctx context.Context, id string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM workflows WHERE service_id=$1`, id).Scan(&n)
	return n, err
}

// --- workflows ---

func (s *Store) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	if wf.ServiceID == "" {
		wf.ServiceID = tenant.DefaultID
	}
	def, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	// service_id is deliberately part of the INSERT but NOT the UPDATE
	// clause: once a workflow is created inside a Service, an ordinary
	// save/re-import of that same id can never silently move it to a
	// different Service (isolation rule 4/13) -- only DeleteWorkflow +
	// a fresh import elsewhere can do that.
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workflows (id, name, service_id, active, definition, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (id) DO UPDATE SET name = $2, active = $4, definition = $5, updated_at = now()
	`, wf.ID, wf.Name, wf.ServiceID, wf.Active, def)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workflow_static_data (workflow_id, data) VALUES ($1, '{}'::jsonb)
		ON CONFLICT (workflow_id) DO NOTHING
	`, wf.ID)
	return err
}

func (s *Store) LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
	var def []byte
	var serviceID string
	if err := s.pool.QueryRow(ctx, `SELECT definition, service_id FROM workflows WHERE id=$1`, id).Scan(&def, &serviceID); err != nil {
		return nil, err
	}
	var wf model.Workflow
	if err := json.Unmarshal(def, &wf); err != nil {
		return nil, err
	}
	// service_id from the column, not the embedded JSON, is always the
	// source of truth -- see SaveWorkflow's ON CONFLICT clause above.
	wf.ServiceID = serviceID
	return &wf, nil
}

// ListWorkflows returns every saved workflow across every Service, used
// at server startup to register schedules (from each workflow's
// Schedule Trigger nodes) and webhook routes (from Webhook Trigger
// nodes) -- see cmd/server/main.go. Deliberately not Service-scoped:
// startup registration must cover every Service's triggers.
func (s *Store) ListWorkflows(ctx context.Context) ([]*model.Workflow, error) {
	rows, err := s.pool.Query(ctx, `SELECT definition, service_id FROM workflows`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Workflow
	for rows.Next() {
		var def []byte
		var serviceID string
		if err := rows.Scan(&def, &serviceID); err != nil {
			return nil, err
		}
		var wf model.Workflow
		if err := json.Unmarshal(def, &wf); err != nil {
			return nil, err
		}
		wf.ServiceID = serviceID
		out = append(out, &wf)
	}
	return out, rows.Err()
}

// ListWorkflowsByService returns only the workflows belonging to one
// Service, ordered oldest first -- the Service Dashboard's workflow
// list, and what Run Service / Run All Services iterate over. This is
// the enforcement point for isolation rule 4 on the read side: a
// Service's dashboard only ever learns about its own workflows.
func (s *Store) ListWorkflowsByService(ctx context.Context, serviceID string) ([]*model.Workflow, error) {
	rows, err := s.pool.Query(ctx, `SELECT definition, service_id FROM workflows WHERE service_id=$1 ORDER BY created_at ASC`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*model.Workflow{}
	for rows.Next() {
		var def []byte
		var svcID string
		if err := rows.Scan(&def, &svcID); err != nil {
			return nil, err
		}
		var wf model.Workflow
		if err := json.Unmarshal(def, &wf); err != nil {
			return nil, err
		}
		wf.ServiceID = svcID
		out = append(out, &wf)
	}
	return out, rows.Err()
}

// ErrWorkflowNotFound is returned by DeleteWorkflow when no workflow
// with the given id exists (the API maps this to HTTP 404).
var ErrWorkflowNotFound = errors.New("store: workflow not found")

// ErrWorkflowHasActiveExecutions is returned by DeleteWorkflow when the
// workflow still has a queued/running/waiting execution (the API maps
// this to HTTP 409). Deleting the workflow row out from under a live
// execution would either fail loudly mid-run (once ON DELETE CASCADE
// removes workflow_static_data -- see WithLock, which requires that
// row to exist) or, worse, succeed while a goroutine is still reading/
// writing it -- rule 13: prevent race condition and data corruption.
var ErrWorkflowHasActiveExecutions = errors.New("store: workflow has a running or queued execution and cannot be deleted")

// DeleteWorkflow removes a workflow and everything scoped to it
// (workflow_static_data, executions, credentials, schedules -- all
// declared ON DELETE CASCADE in schema.sql, so one statement here is
// enough; no N+1 cleanup queries needed) in a single transaction.
//
// Safety, matching this package's existing WithLock pattern:
//  1. SELECT ... FOR UPDATE locks the workflow row first, so a
//     concurrent SaveWorkflow/DeleteWorkflow/WithLock call for the same
//     id serializes against this one instead of racing it.
//  2. With that lock held, count executions still in flight
//     (status IN queued/running/waiting AND finished_at IS NULL --
//     the same predicate MarkInterruptedExecutions uses at startup).
//     Any match aborts the whole transaction via
//     ErrWorkflowHasActiveExecutions -- the row lock means no new
//     execution row can be inserted for this workflow between this
//     check and the DELETE below.
//  3. Only then DELETE FROM workflows, relying on the FK cascades for
//     the rest.
//
// This is the durable half of the delete-safety check; the API layer
// also consults runner.Runner.IsWorkflowActive for a fast in-memory
// check that additionally covers scheduler/webhook-triggered runs,
// which (unlike Manager-started runs) write no Postgres row at all
// until they finish -- see runner.RunFromNode/runOnce.
func (s *Store) DeleteWorkflow(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, `SELECT true FROM workflows WHERE id=$1 FOR UPDATE`, id).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWorkflowNotFound
		}
		return err
	}

	var activeCount int
	err = tx.QueryRow(ctx, `
		SELECT count(*) FROM executions
		WHERE workflow_id = $1 AND finished_at IS NULL
		  AND status IN ('queued', 'running', 'waiting')
	`, id).Scan(&activeCount)
	if err != nil {
		return err
	}
	if activeCount > 0 {
		return ErrWorkflowHasActiveExecutions
	}

	if _, err := tx.Exec(ctx, `DELETE FROM workflows WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- static data (engine.StaticDataStore) ---

// WithLock takes a Postgres row lock (SELECT ... FOR UPDATE) for the
// duration of fn so two concurrent executions of the same workflow
// cannot race on shared counters/dedup state/model-fallback queues
// (rule 13: "prevent race condition and data corruption").
func (s *Store) WithLock(ctx context.Context, workflowID string, fn func(data map[string]any) (map[string]any, error)) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var raw []byte
	err = tx.QueryRow(ctx, `SELECT data FROM workflow_static_data WHERE workflow_id=$1 FOR UPDATE`, workflowID).Scan(&raw)
	if err != nil {
		return fmt.Errorf("store: static data row missing for workflow %q (was it saved via SaveWorkflow?): %w", workflowID, err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return err
	}
	if data == nil {
		data = map[string]any{}
	}

	newData, err := fn(data)
	if err != nil {
		return err
	}
	newRaw, err := json.Marshal(newData)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_static_data SET data=$1, updated_at=now() WHERE workflow_id=$2`, newRaw, workflowID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- executions ---

// maxNodeRuns bounds how much per-execution log detail is persisted,
// matching rule 18 ("limited logs"). The full in-memory execution
// during a run is unaffected; this only caps what's written to
// Postgres for history.
const maxNodeRuns = 500

func (s *Store) SaveExecution(ctx context.Context, ex *model.Execution) error {
	runs := ex.NodeRuns
	if len(runs) > maxNodeRuns {
		runs = runs[len(runs)-maxNodeRuns:]
	}
	nodeRunsJSON, err := json.Marshal(runs)
	if err != nil {
		return err
	}
	serviceID := ex.ServiceID
	if serviceID == "" {
		// Old caller/test that never set ServiceID: look it up from the
		// owning workflow rather than defaulting blind, so history stays
		// correctly attributed even for code paths not yet updated.
		_ = s.pool.QueryRow(ctx, `SELECT service_id FROM workflows WHERE id=$1`, ex.WorkflowID).Scan(&serviceID)
		if serviceID == "" {
			serviceID = tenant.DefaultID
		}
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO executions (id, workflow_id, service_id, mode, status, started_at, finished_at, error, node_runs)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET status=$5, finished_at=$7, error=$8, node_runs=$9
	`, ex.ID, ex.WorkflowID, serviceID, ex.Mode, ex.Status, ex.StartedAt, ex.FinishedAt, ex.Error, nodeRunsJSON)
	return err
}

// GetExecution loads one persisted execution by id -- the durable
// fallback GET /api/executions/{id} uses once an async run's in-memory
// record has been evicted from internal/runner.Manager (process
// restart, or simply old enough to have rolled off the in-memory
// cache). Returns the same *model.Execution shape SaveExecution wrote,
// node_runs included (already capped at maxNodeRuns by SaveExecution).
// ListExecutions returns recent execution history, newest first. The
// retention cleaner removes terminal records older than 12 hours; the
// limit keeps the UI response bounded even before cleanup runs.
func (s *Store) ListExecutions(ctx context.Context, limit int) ([]*model.Execution, error) {
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_id, service_id, mode, status, started_at, finished_at, error, node_runs
		FROM executions
		ORDER BY started_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*model.Execution, 0, limit)
	for rows.Next() {
		var ex model.Execution
		var nodeRunsJSON []byte
		if err := rows.Scan(&ex.ID, &ex.WorkflowID, &ex.ServiceID, &ex.Mode, &ex.Status, &ex.StartedAt, &ex.FinishedAt, &ex.Error, &nodeRunsJSON); err != nil {
			return nil, err
		}
		if len(nodeRunsJSON) > 0 {
			if err := json.Unmarshal(nodeRunsJSON, &ex.NodeRuns); err != nil {
				return nil, fmt.Errorf("store: decode node_runs for execution %q: %w", ex.ID, err)
			}
		}
		out = append(out, &ex)
	}
	return out, rows.Err()
}

// ListExecutionsByService is ListExecutions scoped to one Service --
// backs the Service Dashboard's execution/status list (isolation rule
// 4/5 on the read side).
func (s *Store) ListExecutionsByService(ctx context.Context, serviceID string, limit int) ([]*model.Execution, error) {
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_id, service_id, mode, status, started_at, finished_at, error, node_runs
		FROM executions
		WHERE service_id = $1
		ORDER BY started_at DESC
		LIMIT $2
	`, serviceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*model.Execution, 0, limit)
	for rows.Next() {
		var ex model.Execution
		var nodeRunsJSON []byte
		if err := rows.Scan(&ex.ID, &ex.WorkflowID, &ex.ServiceID, &ex.Mode, &ex.Status, &ex.StartedAt, &ex.FinishedAt, &ex.Error, &nodeRunsJSON); err != nil {
			return nil, err
		}
		if len(nodeRunsJSON) > 0 {
			if err := json.Unmarshal(nodeRunsJSON, &ex.NodeRuns); err != nil {
				return nil, fmt.Errorf("store: decode node_runs for execution %q: %w", ex.ID, err)
			}
		}
		out = append(out, &ex)
	}
	return out, rows.Err()
}

// DeleteExecutionsBefore permanently removes terminal execution history
// older than cutoff. Running/queued records are deliberately retained so
// the cleanup job can never delete an execution that is still in flight.
// MarkInterruptedExecutions marks non-terminal executions left behind by a
// previous server process as cancelled. The async Manager is in-memory, so
// after a restart those queued/running records cannot be resumed or cancelled
// by the new process; keeping them as active would make the live monitor lie
// and leave stale Cancel buttons forever.
func (s *Store) MarkInterruptedExecutions(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE executions
		SET status='cancelled', finished_at=COALESCE(finished_at, now()),
			error=CASE WHEN error IS NULL OR error='' THEN 'execution interrupted by server restart' ELSE error END
		WHERE status IN ('queued', 'running', 'waiting') AND finished_at IS NULL
	`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DeleteExecution permanently removes one terminal execution immediately.
// The execution_checkpoints row is removed by its ON DELETE CASCADE FK as a
// final safety net; callers also explicitly delete checkpoints for clear logs.
// This keeps PostgreSQL storage flat even when executions are created frequently.
func (s *Store) DeleteExecution(ctx context.Context, executionID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM executions WHERE id=$1`, executionID)
	return err
}

func (s *Store) DeleteExecutionsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM executions
		WHERE finished_at IS NOT NULL AND finished_at < $1
	`, cutoff)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) GetExecution(ctx context.Context, id string) (*model.Execution, error) {
	var ex model.Execution
	var nodeRunsJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, workflow_id, service_id, mode, status, started_at, finished_at, error, node_runs
		FROM executions WHERE id=$1
	`, id).Scan(&ex.ID, &ex.WorkflowID, &ex.ServiceID, &ex.Mode, &ex.Status, &ex.StartedAt, &ex.FinishedAt, &ex.Error, &nodeRunsJSON)
	if err != nil {
		return nil, err
	}
	if len(nodeRunsJSON) > 0 {
		if err := json.Unmarshal(nodeRunsJSON, &ex.NodeRuns); err != nil {
			return nil, fmt.Errorf("store: decode node_runs for execution %q: %w", id, err)
		}
	}
	return &ex, nil
}

// --- credentials (vault.Store) ---

func (s *Store) GetEncrypted(ctx context.Context, workflowID, logicalName string) ([]byte, error) {
	var ct []byte
	err := s.pool.QueryRow(ctx, `SELECT ciphertext FROM credentials WHERE workflow_id=$1 AND logical_name=$2`, workflowID, logicalName).Scan(&ct)
	return ct, err
}

func (s *Store) PutEncrypted(ctx context.Context, workflowID, logicalName string, ciphertext []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO credentials (workflow_id, logical_name, ciphertext, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (workflow_id, logical_name) DO UPDATE SET ciphertext=$3, updated_at=now()
	`, workflowID, logicalName, ciphertext)
	return err
}

// --- central Google account credential (vault.AccountStore) ---
//
// Deliberately separate from the per-workflow `credentials` table/
// methods above: this is the single, workflow-independent Google
// account credential every Google node falls back to (see
// internal/vault/central.go). Same encryption path (same *vault.Vault
// AEAD/master key), different table, no workflow_id/FK -- it must
// survive workflows being deleted.

func (s *Store) GetEncryptedAccount(ctx context.Context, account string) ([]byte, error) {
	var ct []byte
	err := s.pool.QueryRow(ctx, `SELECT ciphertext FROM google_account_credentials WHERE account=$1`, account).Scan(&ct)
	return ct, err
}

func (s *Store) PutEncryptedAccount(ctx context.Context, account string, ciphertext []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO google_account_credentials (account, ciphertext, updated_at)
		VALUES ($1,$2, now())
		ON CONFLICT (account) DO UPDATE SET ciphertext=$2, updated_at=now()
	`, account, ciphertext)
	return err
}

func (s *Store) DeleteEncryptedAccount(ctx context.Context, account string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM google_account_credentials WHERE account=$1`, account)
	return err
}

// AccountCredentialMeta never returns a "not found" error -- an absent
// row just means "not configured yet" (exists=false), which is a normal
// state for the frontend's status badge to render, not a failure. Any
// other query error is still surfaced as exists=false/err!=nil so a
// transient DB problem doesn't get reported as "not configured".
func (s *Store) AccountCredentialMeta(ctx context.Context, account string) (time.Time, bool, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx, `SELECT updated_at FROM google_account_credentials WHERE account=$1`, account).Scan(&t)
	if err != nil {
		if err == pgx.ErrNoRows {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	return t, true, nil
}

// CredentialInfo is a credential's metadata with no secret material --
// safe to return from an HTTP endpoint (rule 11/12). NodeName is the
// vault's logical_name (== the node's Name in the workflow).
type CredentialInfo struct {
	NodeName  string    `json:"nodeName"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ListCredentials returns which nodes in a workflow have a stored
// credential and when it was last written -- deliberately selects only
// logical_name/updated_at, never the ciphertext column, so this query
// can never become an accidental secret-leak path no matter how its
// result gets serialized upstream.
func (s *Store) ListCredentials(ctx context.Context, workflowID string) ([]CredentialInfo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT logical_name, updated_at FROM credentials
		WHERE workflow_id = $1
		ORDER BY logical_name
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CredentialInfo
	for rows.Next() {
		var ci CredentialInfo
		if err := rows.Scan(&ci.NodeName, &ci.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, ci)
	}
	return out, rows.Err()
}

// --- Global / Service Environment (vault.EnvStore) ---
//
// Both tables store AEAD ciphertext only, same cipher/master key as
// every other secret in this codebase (see vault.EnvVault) -- rule 2:
// "Sensitive values নিরাপদভাবে store করবে". EnvInfo below is the only
// shape ever handed back over HTTP: name/is_secret/updated_at, never
// the value (rule 13 applies to configuration the same as credentials).

// EnvInfo is one environment entry's metadata, safe to serialize to
// JSON -- never the decrypted value.
type EnvInfo struct {
	Key       string    `json:"key"`
	IsSecret  bool      `json:"isSecret"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (s *Store) PutGlobalEnv(ctx context.Context, key string, ciphertext []byte, isSecret bool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO global_env (key, ciphertext, is_secret, updated_at) VALUES ($1,$2,$3,now())
		ON CONFLICT (key) DO UPDATE SET ciphertext=$2, is_secret=$3, updated_at=now()
	`, key, ciphertext, isSecret)
	return err
}

func (s *Store) GetGlobalEnv(ctx context.Context, key string) ([]byte, error) {
	var ct []byte
	err := s.pool.QueryRow(ctx, `SELECT ciphertext FROM global_env WHERE key=$1`, key).Scan(&ct)
	return ct, err
}

func (s *Store) DeleteGlobalEnv(ctx context.Context, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM global_env WHERE key=$1`, key)
	return err
}

// ListGlobalEnv returns every configured Global Environment key's
// metadata (never values), ordered by key for a stable dashboard list.
func (s *Store) ListGlobalEnv(ctx context.Context) ([]EnvInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, is_secret, updated_at FROM global_env ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EnvInfo{}
	for rows.Next() {
		var e EnvInfo
		if err := rows.Scan(&e.Key, &e.IsSecret, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllGlobalEnvCiphertext loads every Global Environment row's raw
// ciphertext in one query -- used once per workflow run (see
// vault.EnvVault.ResolveAll) rather than one query per $env lookup, so
// a Code node reading several allowlisted names inside a hot per-item
// loop never causes N database round trips per node.
func (s *Store) AllGlobalEnvCiphertext(ctx context.Context) (map[string][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, ciphertext FROM global_env`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var key string
		var ct []byte
		if err := rows.Scan(&key, &ct); err != nil {
			return nil, err
		}
		out[key] = ct
	}
	return out, rows.Err()
}

func (s *Store) PutServiceEnv(ctx context.Context, serviceID, key string, ciphertext []byte, isSecret bool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO service_env (service_id, key, ciphertext, is_secret, updated_at) VALUES ($1,$2,$3,$4,now())
		ON CONFLICT (service_id, key) DO UPDATE SET ciphertext=$3, is_secret=$4, updated_at=now()
	`, serviceID, key, ciphertext, isSecret)
	return err
}

func (s *Store) DeleteServiceEnv(ctx context.Context, serviceID, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM service_env WHERE service_id=$1 AND key=$2`, serviceID, key)
	return err
}

// DeleteAllServiceEnv removes every Environment row of ONE Service in a
// single SQL statement, so it is all-or-nothing (Postgres runs one
// statement atomically) -- no partial delete is possible. Scoped strictly
// by service_id; returns how many rows were removed.
func (s *Store) DeleteAllServiceEnv(ctx context.Context, serviceID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM service_env WHERE service_id=$1`, serviceID)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) ListServiceEnv(ctx context.Context, serviceID string) ([]EnvInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, is_secret, updated_at FROM service_env WHERE service_id=$1 ORDER BY key`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EnvInfo{}
	for rows.Next() {
		var e EnvInfo
		if err := rows.Scan(&e.Key, &e.IsSecret, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllServiceEnvCiphertext mirrors AllGlobalEnvCiphertext, scoped to one
// Service -- the Service-Environment half of the precedence chain.
func (s *Store) AllServiceEnvCiphertext(ctx context.Context, serviceID string) (map[string][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, ciphertext FROM service_env WHERE service_id=$1`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var key string
		var ct []byte
		if err := rows.Scan(&key, &ct); err != nil {
			return nil, err
		}
		out[key] = ct
	}
	return out, rows.Err()
}

// --- Run All Services (durable sequential sweep -- runall.Manager) ---

// ErrRunAllAlreadyActive maps the schema's partial unique index
// violation to a clear, typed error the API turns into 409 (rule:
// "Duplicate Run All execution prevent করো").
var ErrRunAllAlreadyActive = errors.New("store: a Run All Services sweep is already queued or running")

// RunAllJobRow is the durable row backing one Run All Services sweep.
// Results is kept as raw JSON (not unmarshaled here) because its shape
// is owned by package runall, not store -- store only needs to persist
// and hand back opaque bytes.
type RunAllJobRow struct {
	ID            string
	Status        string
	ServiceIDs    []string
	StopOnFailure bool
	Results       json.RawMessage
	Error         string
	StartedAt     time.Time
	UpdatedAt     time.Time
	FinishedAt    *time.Time
}

func (s *Store) CreateRunAllJob(ctx context.Context, id string, serviceIDs []string, stopOnFailure bool) error {
	idsJSON, err := json.Marshal(serviceIDs)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO run_all_jobs (id, status, service_ids, stop_on_failure, results, started_at, updated_at)
		VALUES ($1, 'queued', $2, $3, '[]'::jsonb, now(), now())
	`, id, idsJSON, stopOnFailure)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrRunAllAlreadyActive
		}
		return err
	}
	return nil
}

// UpdateRunAllJob persists progress -- called after every service/
// workflow step completes so a restart can resume from `results`
// instead of re-running already-recorded work (rule 17).
func (s *Store) UpdateRunAllJob(ctx context.Context, id, status string, results json.RawMessage, jobErr string) error {
	var finishedAt any
	if status == "success" || status == "error" || status == "cancelled" {
		finishedAt = time.Now()
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE run_all_jobs SET status=$2, results=$3, error=$4, updated_at=now(), finished_at=COALESCE($5, finished_at)
		WHERE id=$1
	`, id, status, results, nullableString(jobErr), finishedAt)
	if err != nil {
		return err
	}
	if finishedAt != nil {
		// Retention (rule 10/11): keep only the 20 newest finished sweeps so
		// this table never accumulates; active rows are never touched.
		_, _ = s.pool.Exec(ctx, `
			DELETE FROM run_all_jobs
			WHERE status NOT IN ('queued','running')
			  AND id NOT IN (SELECT id FROM run_all_jobs ORDER BY started_at DESC LIMIT 20)
		`)
	}
	return nil
}

func (s *Store) GetRunAllJob(ctx context.Context, id string) (*RunAllJobRow, error) {
	return s.scanRunAllJob(s.pool.QueryRow(ctx, `
		SELECT id, status, service_ids, stop_on_failure, results, COALESCE(error,''), started_at, updated_at, finished_at
		FROM run_all_jobs WHERE id=$1`, id))
}

// GetActiveRunAllJob returns the currently queued/running sweep, if
// any -- what a restart's recovery pass resumes, and what the
// dashboard polls for live progress. Returns (nil, nil) when idle.
func (s *Store) GetActiveRunAllJob(ctx context.Context) (*RunAllJobRow, error) {
	row, err := s.scanRunAllJob(s.pool.QueryRow(ctx, `
		SELECT id, status, service_ids, stop_on_failure, results, COALESCE(error,''), started_at, updated_at, finished_at
		FROM run_all_jobs WHERE status IN ('queued','running') LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return row, err
}

func (s *Store) scanRunAllJob(row pgx.Row) (*RunAllJobRow, error) {
	var j RunAllJobRow
	var idsJSON []byte
	if err := row.Scan(&j.ID, &j.Status, &idsJSON, &j.StopOnFailure, &j.Results, &j.Error, &j.StartedAt, &j.UpdatedAt, &j.FinishedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(idsJSON, &j.ServiceIDs); err != nil {
		return nil, err
	}
	return &j, nil
}

// ListRunAllJobs returns recent sweeps, newest first, for a simple
// history view (bounded -- rule 10/18).
func (s *Store) ListRunAllJobs(ctx context.Context, limit int) ([]*RunAllJobRow, error) {
	if limit < 1 || limit > 50 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, status, service_ids, stop_on_failure, results, COALESCE(error,''), started_at, updated_at, finished_at
		FROM run_all_jobs ORDER BY started_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*RunAllJobRow{}
	for rows.Next() {
		j, err := s.scanRunAllJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

// --- Run All Services Schedule (fires runall.Manager.StartAll on a
// cron/interval schedule -- see internal/scheduler's Schedule.RunAll
// field; no new runner/queue, the exact same scheduler tick loop and
// the exact same Run All Services path a manual click uses) ---

// ErrRunAllScheduleNotFound is returned by Update/Delete when the id
// does not exist (already deleted, or never existed).
var ErrRunAllScheduleNotFound = errors.New("store: run-all schedule not found")

// RunAllScheduleRow is one persisted "Run All Services" schedule.
// Exactly one of CronExpr/IntervalSeconds is expected to be set,
// mirroring a Schedule Trigger node's own two shapes.
type RunAllScheduleRow struct {
	ID              string
	Label           string
	CronExpr        string
	IntervalSeconds int
	StopOnFailure   bool
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time

	// Optional window and repeat limit. nil = no bound; MaxRuns 0 =
	// unlimited. RunCount is how many times it has fired so far.
	StartAt  *time.Time
	EndAt    *time.Time
	MaxRuns  int
	RunCount int
}

func nullableInt(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

// CreateRunAllSchedule inserts a new schedule. Callers (see
// internal/api) are expected to pass Enabled=false for a brand new
// schedule -- the schema also defaults enabled to false so this holds
// even if a caller forgets.
func (s *Store) CreateRunAllSchedule(ctx context.Context, row RunAllScheduleRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO run_all_schedules (id, label, cron_expr, interval_seconds, stop_on_failure, enabled, start_at, end_at, max_runs, run_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, now(), now())
	`, row.ID, row.Label, nullableString(row.CronExpr), nullableInt(row.IntervalSeconds), row.StopOnFailure, row.Enabled, row.StartAt, row.EndAt, row.MaxRuns)
	return err
}

// UpdateRunAllSchedule replaces every mutable field of an existing
// schedule (label, timing, stop-on-failure, enabled) in one call, so
// Edit and Enable/Disable both go through the same path.
func (s *Store) UpdateRunAllSchedule(ctx context.Context, row RunAllScheduleRow) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE run_all_schedules
		SET label=$2, cron_expr=$3, interval_seconds=$4, stop_on_failure=$5, enabled=$6,
		    start_at=$7, end_at=$8, max_runs=$9, run_count=$10, updated_at=now()
		WHERE id=$1
	`, row.ID, row.Label, nullableString(row.CronExpr), nullableInt(row.IntervalSeconds), row.StopOnFailure, row.Enabled, row.StartAt, row.EndAt, row.MaxRuns, row.RunCount)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrRunAllScheduleNotFound
	}
	return nil
}

func (s *Store) DeleteRunAllSchedule(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM run_all_schedules WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrRunAllScheduleNotFound
	}
	return nil
}

func (s *Store) GetRunAllSchedule(ctx context.Context, id string) (*RunAllScheduleRow, error) {
	row, err := s.scanRunAllSchedule(s.pool.QueryRow(ctx, `
		SELECT id, label, COALESCE(cron_expr,''), COALESCE(interval_seconds,0), stop_on_failure, enabled, created_at, updated_at, start_at, end_at, max_runs, run_count
		FROM run_all_schedules WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRunAllScheduleNotFound
	}
	return row, err
}

// ListRunAllSchedules returns every schedule, oldest first (stable
// creation order, matching how Services/workflows are listed
// elsewhere in this package).
func (s *Store) ListRunAllSchedules(ctx context.Context) ([]RunAllScheduleRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, label, COALESCE(cron_expr,''), COALESCE(interval_seconds,0), stop_on_failure, enabled, created_at, updated_at, start_at, end_at, max_runs, run_count
		FROM run_all_schedules ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunAllScheduleRow{}
	for rows.Next() {
		r, err := s.scanRunAllSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// RecordRunAllScheduleFire counts one fire of a schedule and, when its
// Repeat limit is reached, disables it (atomic, so a restart never loses
// or double-counts a run).
func (s *Store) RecordRunAllScheduleFire(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE run_all_schedules
		SET run_count = run_count + 1,
		    enabled = CASE WHEN max_runs > 0 AND run_count + 1 >= max_runs THEN false ELSE enabled END,
		    updated_at = now()
		WHERE id=$1`, id)
	return err
}

func (s *Store) scanRunAllSchedule(row pgx.Row) (*RunAllScheduleRow, error) {
	var r RunAllScheduleRow
	if err := row.Scan(&r.ID, &r.Label, &r.CronExpr, &r.IntervalSeconds, &r.StopOnFailure, &r.Enabled, &r.CreatedAt, &r.UpdatedAt, &r.StartAt, &r.EndAt, &r.MaxRuns, &r.RunCount); err != nil {
		return nil, err
	}
	return &r, nil
}

// SaveExecutionCheckpoint upserts the single latest checkpoint for an
// execution. The JSON representation is bounded before it reaches Postgres.
func (s *Store) SaveExecutionCheckpoint(ctx context.Context, cp *model.ExecutionCheckpoint) error {
	if cp == nil || cp.ExecutionID == "" || cp.WorkflowID == "" {
		return errors.New("store: invalid execution checkpoint")
	}
	if cp.State.Version == 0 {
		cp.State.Version = 1
	}
	payload, err := json.Marshal(cp.State)
	if err != nil {
		return err
	}
	maxBytes := 512 * 1024
	if v := os.Getenv("MICROFLOW_MAX_CHECKPOINT_BYTES"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 64*1024 && n <= 4*1024*1024 {
			maxBytes = n
		}
	}
	if len(payload) > maxBytes {
		return fmt.Errorf("execution checkpoint %q: %w: %d > %d bytes", cp.ExecutionID, model.ErrCheckpointTooLarge, len(payload), maxBytes)
	}
	cp.UpdatedAt = time.Now()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO execution_checkpoints
		(execution_id, workflow_id, workflow_hash, workflow_updated_at, mode, status, version, payload, payload_bytes, lease_owner, lease_until, started_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),now())
		ON CONFLICT (execution_id) DO UPDATE SET
		workflow_id=EXCLUDED.workflow_id,
		workflow_hash=EXCLUDED.workflow_hash,
		workflow_updated_at=EXCLUDED.workflow_updated_at,
		mode=EXCLUDED.mode,
		status=EXCLUDED.status,
		version=EXCLUDED.version,
		payload=EXCLUDED.payload,
		payload_bytes=EXCLUDED.payload_bytes,
		lease_owner=EXCLUDED.lease_owner,
		lease_until=EXCLUDED.lease_until,
		started_at=EXCLUDED.started_at,
		updated_at=now()
	`, cp.ExecutionID, cp.WorkflowID, cp.WorkflowHash, cp.WorkflowUpdatedAt, cp.Mode, cp.State.Status, cp.State.Version, payload, len(payload), nullableString(cp.LeaseOwner), nullableTime(cp.LeaseUntil), cp.StartedAt)
	return err
}

func (s *Store) DeleteExecutionCheckpoint(ctx context.Context, executionID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM execution_checkpoints WHERE execution_id=$1`, executionID)
	return err
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v
}

// ClaimRecoverableExecutionCheckpoints atomically leases a bounded batch of
// active checkpoints. FOR UPDATE SKIP LOCKED makes duplicate startup workers
// harmless; an expired lease can be reclaimed after a crashed worker.
func (s *Store) ClaimRecoverableExecutionCheckpoints(ctx context.Context, owner string, limit int, lease time.Duration) ([]*model.ExecutionCheckpoint, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 16 {
		limit = 16
	}
	if lease <= 0 {
		lease = time.Hour
	}
	rows, err := s.pool.Query(ctx, `
		WITH candidates AS (
			SELECT c.execution_id
			FROM execution_checkpoints c
			JOIN executions e ON e.id=c.execution_id
			WHERE e.status IN ('queued','running','waiting')
			  AND e.finished_at IS NULL
			  AND (c.lease_until IS NULL OR c.lease_until < now())
			ORDER BY c.updated_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE execution_checkpoints c
		SET lease_owner=$2, lease_until=now()+$3::interval, updated_at=now()
		FROM candidates x
		WHERE c.execution_id=x.execution_id
		RETURNING c.execution_id,c.workflow_id,c.workflow_hash,c.workflow_updated_at,c.mode,c.started_at,c.version,c.payload,c.updated_at,c.lease_owner,c.lease_until
	`, limit, owner, fmt.Sprintf("%d seconds", int(lease.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*model.ExecutionCheckpoint, 0, limit)
	for rows.Next() {
		var cp model.ExecutionCheckpoint
		var payload []byte
		if err := rows.Scan(&cp.ExecutionID, &cp.WorkflowID, &cp.WorkflowHash, &cp.WorkflowUpdatedAt, &cp.Mode, &cp.StartedAt, &cp.State.Version, &payload, &cp.UpdatedAt, &cp.LeaseOwner, &cp.LeaseUntil); err != nil {
			return nil, err
		}
		if len(payload) == 0 {
			return nil, fmt.Errorf("store: empty checkpoint payload for %q", cp.ExecutionID)
		}
		if len(payload) > checkpointMaxBytesForStore() {
			return nil, fmt.Errorf("store: checkpoint %q exceeds configured size", cp.ExecutionID)
		}
		if err := json.Unmarshal(payload, &cp.State); err != nil {
			return nil, fmt.Errorf("store: decode checkpoint %q: %w", cp.ExecutionID, err)
		}
		out = append(out, &cp)
	}
	return out, rows.Err()
}

func checkpointMaxBytesForStore() int {
	const def = 512 * 1024
	if v := os.Getenv("MICROFLOW_MAX_CHECKPOINT_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 64*1024 && n <= 4*1024*1024 {
			return n
		}
	}
	return def
}

func (s *Store) ClearExecutionCheckpointLease(ctx context.Context, executionID, owner string) error {
	_, err := s.pool.Exec(ctx, `UPDATE execution_checkpoints SET lease_owner=NULL, lease_until=NULL WHERE execution_id=$1 AND lease_owner=$2`, executionID, owner)
	return err
}

// CleanupOrphanedExecutionCheckpoints removes only a small bounded batch of
// checkpoints that are no longer recoverable. It never scans/deletes the whole
// table in one operation.
func (s *Store) CleanupOrphanedExecutionCheckpoints(ctx context.Context, batch int, cutoff time.Time) (int64, error) {
	if batch < 1 {
		batch = 20
	}
	if batch > 100 {
		batch = 100
	}
	tag, err := s.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT c.execution_id
			FROM execution_checkpoints c
			LEFT JOIN executions e ON e.id=c.execution_id
			LEFT JOIN workflows w ON w.id=c.workflow_id
			WHERE e.id IS NULL OR w.id IS NULL
			   OR e.status IN ('success','error','cancelled')
			   OR (c.updated_at < $1 AND e.status NOT IN ('queued','running','waiting'))
			ORDER BY c.updated_at ASC
			LIMIT $2
		)
		DELETE FROM execution_checkpoints c USING doomed d WHERE c.execution_id=d.execution_id
	`, cutoff, batch)
	return tag.RowsAffected(), err
}
