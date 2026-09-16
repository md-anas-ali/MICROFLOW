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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"microflow/internal/model"
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

// --- workflows ---

func (s *Store) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	def, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workflows (id, name, active, definition, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (id) DO UPDATE SET name = $2, active = $3, definition = $4, updated_at = now()
	`, wf.ID, wf.Name, wf.Active, def)
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
	if err := s.pool.QueryRow(ctx, `SELECT definition FROM workflows WHERE id=$1`, id).Scan(&def); err != nil {
		return nil, err
	}
	var wf model.Workflow
	if err := json.Unmarshal(def, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

// ListWorkflows returns every saved workflow, used at server startup to
// register schedules (from each workflow's Schedule Trigger nodes) and
// webhook routes (from Webhook Trigger nodes) -- see cmd/server/main.go.
func (s *Store) ListWorkflows(ctx context.Context) ([]*model.Workflow, error) {
	rows, err := s.pool.Query(ctx, `SELECT definition FROM workflows`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Workflow
	for rows.Next() {
		var def []byte
		if err := rows.Scan(&def); err != nil {
			return nil, err
		}
		var wf model.Workflow
		if err := json.Unmarshal(def, &wf); err != nil {
			return nil, err
		}
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
	_, err = s.pool.Exec(ctx, `
		INSERT INTO executions (id, workflow_id, mode, status, started_at, finished_at, error, node_runs)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET status=$4, finished_at=$6, error=$7, node_runs=$8
	`, ex.ID, ex.WorkflowID, ex.Mode, ex.Status, ex.StartedAt, ex.FinishedAt, ex.Error, nodeRunsJSON)
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
		SELECT id, workflow_id, mode, status, started_at, finished_at, error, node_runs
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
		if err := rows.Scan(&ex.ID, &ex.WorkflowID, &ex.Mode, &ex.Status, &ex.StartedAt, &ex.FinishedAt, &ex.Error, &nodeRunsJSON); err != nil {
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
		SELECT id, workflow_id, mode, status, started_at, finished_at, error, node_runs
		FROM executions WHERE id=$1
	`, id).Scan(&ex.ID, &ex.WorkflowID, &ex.Mode, &ex.Status, &ex.StartedAt, &ex.FinishedAt, &ex.Error, &nodeRunsJSON)
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
		return fmt.Errorf("execution checkpoint %q: %w: %d > %d bytes", cp.ExecutionID, ErrCheckpointTooLarge, len(payload), maxBytes)
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
