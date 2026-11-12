package store

import (
	"context"
	"time"
)

// ListStaleExecutionIDs returns up to limit ids of executions that the
// database still shows as queued/running/waiting, started before
// startedBefore, and that have NO recovery checkpoint (rows with a
// checkpoint belong to the recovery loop and are never listed here).
//
// The caller must still check that its own process is not running the id
// before closing it (see runner.ReapStaleExecutions); this query only says
// "the database thinks it is active".
func (s *Store) ListStaleExecutionIDs(ctx context.Context, startedBefore time.Time, limit int) ([]string, error) {
	if limit < 1 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.id
		FROM executions e
		WHERE e.status IN ('queued', 'running', 'waiting')
		  AND e.finished_at IS NULL
		  AND e.started_at < $1
		  AND NOT EXISTS (SELECT 1 FROM execution_checkpoints c WHERE c.execution_id = e.id)
		ORDER BY e.started_at ASC
		LIMIT $2
	`, startedBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FailStaleRunAllJobs closes Run-All sweeps still marked queued/running
// whose progress has not been touched since updatedBefore. A sweep is
// persisted after every step, so a row nobody is advancing would otherwise
// sit "running" forever and, through the single-active unique index, block
// every future Run-All with a 409. Call it only when this process is not
// running a sweep itself.
func (s *Store) FailStaleRunAllJobs(ctx context.Context, updatedBefore time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE run_all_jobs
		SET status='error',
		    finished_at=COALESCE(finished_at, now()),
		    updated_at=now(),
		    error=CASE WHEN error IS NULL OR error='' THEN 'sweep interrupted: no server was running it any more' ELSE error END
		WHERE status IN ('queued', 'running') AND updated_at < $1
	`, updatedBefore)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// CloseActiveExecutions marks every execution of workflowID that the database
// still shows as queued/running/waiting as cancelled and drops its recovery
// checkpoints. Call it only after confirming nothing in this process is really
// running the workflow (runner.IsWorkflowActive == false): such rows are left
// behind by a stopped/killed run and would otherwise block DeleteWorkflow.
func (s *Store) CloseActiveExecutions(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM execution_checkpoints WHERE workflow_id=$1`, workflowID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE executions
		SET status='cancelled', finished_at=COALESCE(finished_at, now()),
		    error=CASE WHEN error IS NULL OR error='' THEN 'stopped before the workflow was replaced' ELSE error END
		WHERE workflow_id=$1 AND finished_at IS NULL AND status IN ('queued','running','waiting')
	`, workflowID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
