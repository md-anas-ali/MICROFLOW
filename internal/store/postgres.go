// Package store is MicroFlow's Postgres persistence layer. It works
// against ANY standard PostgreSQL reachable via DATABASE_URL (Neon,
// Supabase, Render, Railway, self-hosted) — nothing here is
// provider-specific. Uses pgx's connection pool kept intentionally
// small for the 512MB RAM budget (rule 12/18).
package store

import (
	"context"
	"encoding/json"
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

// Open connects using DATABASE_URL and a small pool (default max 4
// connections -- generous for this workload, cheap in RAM; override via
// MICROFLOW_DB_MAX_CONNS if needed).
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: invalid DATABASE_URL: %w", err)
	}
	maxConns := int32(4)
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
