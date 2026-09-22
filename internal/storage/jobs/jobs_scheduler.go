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

// ClaimDueScheduled promotes due scheduled jobs to pending so the scheduler
// can dispatch them to the stream. It claims at most limit jobs with
// scheduled_at <= now in firing order.
//
// Concurrency: SELECT ... FOR UPDATE SKIP LOCKED inside a short tx means any
// number of scheduler instances may run at once — two schedulers never claim
// the same row and no distributed lock is needed (§11.4, §12.3). Each
// promotion appends a scheduled→pending job_events row. All on the write
// pool (PgBouncer-safe: short tx, no session state).
func (s *JobsStore) ClaimDueScheduled(ctx context.Context, now time.Time, limit int) ([]task.Job, error) {
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
		WHERE status = 'scheduled' AND scheduled_at <= $1
		ORDER BY scheduled_at LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due scheduled: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan due job id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("rows due scheduled: %w", err)
	}
	rows.Close()

	out := make([]task.Job, 0, len(ids))
	for _, id := range ids {
		var job task.Job
		if err := scanJobRow(&job, tx.QueryRowContext(ctx,
			`UPDATE jobs SET status = 'pending' WHERE job_id = $1 AND status = 'scheduled'
			RETURNING `+jobColumns, id)); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Lost a race with another claimant; skip.
				continue
			}
			return nil, fmt.Errorf("promote job %s: %w", id, err)
		}
		if err := insertEvent(ctx, tx, id, task.JobScheduled, task.JobPending); err != nil {
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
