package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// ClaimStaleRunning reclaims jobs whose worker died: rows in running whose
// heartbeat_at is older than staleBefore (ARCHITECTURE.md §12.2: 90s, i.e. 3x
// the heartbeat interval, so one missed heartbeat never triggers a false
// reclaim). Reclaimed jobs return to pending with worker and claim token
// cleared, keeping attempt_count/last_error for audit, plus a
// running→pending job_events row. The reaper then re-enqueues them; a healthy
// worker claims them fresh (attempt_count bumps on the new claim).
//
// Concurrency: SELECT ... FOR UPDATE SKIP LOCKED inside a short tx, so any
// number of reaper instances may run at once without double-reclaim. The
// status-guarded UPDATE skips rows another claimant already moved. All on the
// write pool (PgBouncer-safe: short tx, no session state).
func (s *JobsStore) ClaimStaleRunning(ctx context.Context, staleBefore time.Time, limit int) ([]task.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT job_id FROM jobs
		WHERE status = 'running' AND heartbeat_at < $1
		ORDER BY heartbeat_at LIMIT $2 FOR UPDATE SKIP LOCKED`, staleBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("select stale running: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan stale job id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("rows stale running: %w", err)
	}
	rows.Close()

	out := make([]task.Job, 0, len(ids))
	for _, id := range ids {
		var job task.Job
		if err := scanJobRow(&job, tx.QueryRowContext(ctx,
			`UPDATE jobs SET status = 'pending', worker_id = NULL, claim_token = NULL
			WHERE job_id = $1 AND status = 'running'
			RETURNING `+jobColumns, id)); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Lost a race with another claimant; skip.
				continue
			}
			return nil, fmt.Errorf("reclaim job %s: %w", id, err)
		}
		if err := insertEvent(ctx, tx, id, task.JobRunning, task.JobPending); err != nil {
			return nil, err
		}
		hydratePayload(&job)
		out = append(out, job)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}
