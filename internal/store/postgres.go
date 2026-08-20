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
