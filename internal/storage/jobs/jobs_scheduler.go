package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/MikelGV/PierceMQ/internal/cronx"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// ClaimDueScheduled promotes due scheduled jobs to pending so the scheduler
// can dispatch them to the stream. It claims at most limit jobs with
// scheduled_at <= now in firing order.
//
// Recurring series: promoting a series member atomically schedules its
// follow-up (same series_id, next fire time per cron_expr/cron_tz) in the
// same transaction — a crash can never dispatch an occurrence without
// queuing the next, nor queue a follow-up without dispatching the current
// one. The follow-up carries no idempotency key (each occurrence is a fresh
// job) and starts with attempt_count 0.
//
// Concurrency: SELECT ... FOR UPDATE SKIP LOCKED inside a short tx means any
// number of scheduler instances may run at once — two schedulers never claim
// the same row and no distributed lock is needed (§11.4, §12.3). Only the
// claimer creates the follow-up, so occurrences are never duplicated. Each
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
		if err := scheduleFollowup(ctx, tx, job, now); err != nil {
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

// scheduleFollowup queues the next occurrence of a recurring series member.
// No-op for one-shot jobs. Runs inside the claim transaction: promotion and
// follow-up commit atomically.
func scheduleFollowup(ctx context.Context, tx *sql.Tx, parent task.Job, now time.Time) error {
	if !parent.IsRecurring || !parent.CronExpr.Valid || parent.CronExpr.String == "" {
		return nil
	}
	if !parent.SeriesID.Valid {
		return nil
	}
	tz := "UTC"
	if parent.CronTZ.Valid && parent.CronTZ.String != "" {
		tz = parent.CronTZ.String
	}
	next, err := cronx.NextAfter(parent.CronExpr.String, tz, now)
	if err != nil {
		// Unreachable for API-created series (enqueue validates), but a
		// poisoned expression must not fail the whole batch: the current
		// occurrence still dispatches, the series simply ends here.
		return nil
	}
	childID := uuid.New()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, type, payload_ref, queue_name, priority,
			attempt_count, max_retry, owner_user_id,
			cron_expr, cron_tz, series_id, is_recurring,
			scheduled_at, created_at
		) VALUES ($1, 'scheduled', $2, $3, $4, $5, 0, $6, $7, $8, $9, $10, TRUE, $11, $12)`,
		childID,
		parent.Type,
		parent.PayloadRef,
		parent.QueueName,
		parent.Priority,
		parent.MaxRetry,
		parent.OwnerUserID,
		parent.CronExpr,
		tz,
		parent.SeriesID.UUID,
		next.UTC(),
		now.UTC(),
	); err != nil {
		return fmt.Errorf("schedule followup: %w", err)
	}
	if err := insertInitialEvent(ctx, tx, childID, task.JobScheduled); err != nil {
		return err
	}
	return nil
}

// ClaimDueRetries promotes backoff-matured retries: pending/queued rows with
// not_before <= now. It clears the gate (not_before=NULL) so the row is
// dispatched exactly once per maturation; concurrent schedulers are safe via
// FOR UPDATE SKIP LOCKED. Returns claimed jobs for stream dispatch.
func (s *JobsStore) ClaimDueRetries(ctx context.Context, now time.Time, limit int) ([]task.Job, error) {
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
		WHERE status IN ('pending', 'queued') AND not_before IS NOT NULL AND not_before <= $1
		ORDER BY not_before LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due retries: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan due retry id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("rows due retries: %w", err)
	}
	rows.Close()

	out := make([]task.Job, 0, len(ids))
	for _, id := range ids {
		var job task.Job
		if err := scanJobRow(&job, tx.QueryRowContext(ctx,
			`UPDATE jobs SET not_before = NULL WHERE job_id = $1 AND status IN ('pending', 'queued')
			RETURNING `+jobColumns, id)); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("gate retry job %s: %w", id, err)
		}
		hydratePayload(&job)
		out = append(out, job)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}
