package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// ErrNotClaimable is returned when a job cannot transition: already
// running/completed elsewhere, claim token mismatch, or row missing.
// Callers should treat redelivered stream messages for such jobs as
// no-ops (ack + skip). Detect via errors.Is.
var ErrNotClaimable = errors.New("jobs: not claimable")

// claimable are the statuses a worker may move to running.
func claimable(s task.JobStatus) bool {
	return s == task.JobPending || s == task.JobQueued || s == task.JobScheduled
}

func scanJobRow(job *task.Job, row interface {
	Scan(dest ...any) error
}) error {
	return row.Scan(
		&job.JobID,
		&job.Status,
		&job.Type,
		&job.PayloadRef,
		&job.QueueName,
		&job.Priority,
		&job.AttemptCount,
		&job.MaxRetry,
		&job.WorkerID,
		&job.ClaimToken,
		&job.IdempotencyKey,
		&job.ScheduledAt,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
		&job.HeartbeatAt,
		&job.LastError,
	)
}

const jobColumns = `job_id, status, type, payload_ref, queue_name, priority,
	attempt_count, max_retry, worker_id, claim_token, idempotency_key,
	scheduled_at, created_at, started_at, completed_at, heartbeat_at, last_error`

// hydratePayload best-effort decodes payload_ref JSON into Payload so the
// worker handler receives a usable value without a second query.
func hydratePayload(job *task.Job) {
	if job.PayloadRef.Valid && job.PayloadRef.String != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(job.PayloadRef.String), &m); err == nil {
			job.Payload = m
		} else {
			job.Payload = job.PayloadRef.String
		}
	}
}

func insertEvent(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, jobID uuid.UUID, oldStatus, newStatus task.JobStatus) error {
	_, err := q.ExecContext(ctx, `INSERT INTO job_events (
		event_id, job_id, old_status, new_status, occurred_at
	) VALUES (gen_random_uuid(), $1, $2::job_status, $3::job_status, now())`,
		jobID, string(oldStatus), string(newStatus))
	if err != nil {
		return fmt.Errorf("insert job_event: %w", err)
	}
	return nil
}

// ClaimRunning moves a pending/queued/scheduled job to running, assigns
// worker + claim token, stamps started/heartbeat, and bumps attempt_count.
// SELECT ... FOR UPDATE inside a short tx serializes concurrent claimants;
// losers get ErrNotClaimable. All on the write pool (PgBouncer-safe:
// short tx, no session state).
func (s *JobsStore) ClaimRunning(ctx context.Context, jobID uuid.UUID, workerID string) (task.Job, uuid.UUID, error) {
	var out task.Job
	token := uuid.New()

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return out, uuid.Nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var job task.Job
	if err := scanJobRow(&job, tx.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE job_id = $1 FOR UPDATE`, jobID)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, uuid.Nil, ErrNotClaimable
		}
		return out, uuid.Nil, fmt.Errorf("select job for claim: %w", err)
	}
	if !claimable(job.Status) {
		return out, uuid.Nil, ErrNotClaimable
	}
	old := job.Status

	if err := scanJobRow(&out, tx.QueryRowContext(ctx,
		`UPDATE jobs SET status = 'running', worker_id = $2, claim_token = $3,
			started_at = COALESCE(started_at, now()), heartbeat_at = now(),
			attempt_count = attempt_count + 1
		WHERE job_id = $1 RETURNING `+jobColumns, jobID, workerID, token)); err != nil {
		return out, uuid.Nil, fmt.Errorf("claim job: %w", err)
	}

	if err := insertEvent(ctx, tx, jobID, old, task.JobRunning); err != nil {
		return out, uuid.Nil, err
	}
	if err := tx.Commit(); err != nil {
		return out, uuid.Nil, fmt.Errorf("commit: %w", err)
	}

	hydratePayload(&out)
	return out, token, nil
}

// Heartbeat refreshes heartbeat_at for a running job owned by claimToken.
// Zero rows (wrong token, not running) yields ErrNotClaimable.
func (s *JobsStore) Heartbeat(ctx context.Context, jobID, claimToken uuid.UUID) error {
	res, err := s.write.ExecContext(ctx,
		`UPDATE jobs SET heartbeat_at = now()
		WHERE job_id = $1 AND claim_token = $2 AND status = 'running'`,
		jobID, claimToken)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("heartbeat rows: %w", err)
	}
	if n == 0 {
		return ErrNotClaimable
	}
	return nil
}

// Complete moves a running job owned by claimToken to completed.
// Must be called BEFORE acking the Redis message: a crash between leaves
// a completed row whose redeliveries ClaimRunning absorbs via the status
// guard; the reverse order risks acking a never-completed job.
func (s *JobsStore) Complete(ctx context.Context, jobID, claimToken uuid.UUID) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status task.JobStatus
	var token uuid.NullUUID
	if err := tx.QueryRowContext(ctx,
		`SELECT status, claim_token FROM jobs WHERE job_id = $1 FOR UPDATE`, jobID,
	).Scan(&status, &token); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotClaimable
		}
		return fmt.Errorf("select job for complete: %w", err)
	}
	if status != task.JobRunning || !token.Valid || token.UUID != claimToken {
		return ErrNotClaimable
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'completed', completed_at = now() WHERE job_id = $1`,
		jobID); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if err := insertEvent(ctx, tx, jobID, task.JobRunning, task.JobCompleted); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// FailOrRetry records a failed attempt. Below max_retry the job returns to
// pending (claim cleared, worker kept for audit); exhausted jobs move to
// failed with last_error. Returns the updated job.
func (s *JobsStore) FailOrRetry(ctx context.Context, jobID, claimToken uuid.UUID, lastErr string) (task.Job, error) {
	var out task.Job

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var job task.Job
	if err := scanJobRow(&job, tx.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE job_id = $1 FOR UPDATE`, jobID)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, ErrNotClaimable
		}
		return out, fmt.Errorf("select job for fail: %w", err)
	}
	if job.Status != task.JobRunning || !job.ClaimToken.Valid || job.ClaimToken.UUID != claimToken {
		return out, ErrNotClaimable
	}

	next := task.JobPending
	if job.AttemptCount >= job.MaxRetry {
		next = task.JobFailed
	}

	query := `UPDATE jobs SET status = $2, claim_token = NULL, last_error = $3`
	if next == task.JobFailed {
		query += `, completed_at = now()`
	}
	query += ` WHERE job_id = $1 RETURNING ` + jobColumns

	if err := scanJobRow(&out, tx.QueryRowContext(ctx, query,
		jobID, string(next), sql.NullString{String: lastErr, Valid: lastErr != ""})); err != nil {
		return out, fmt.Errorf("fail job: %w", err)
	}
	if err := insertEvent(ctx, tx, jobID, task.JobRunning, next); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("commit: %w", err)
	}

	hydratePayload(&out)
	return out, nil
}

// ListStalePending returns jobs stuck in pending/queued older than the
// cutoff (orphans of a DB-insert/XADD dual-write where the XADD failed).
// Read-only on the read pool; the future enqueue path (or a sweeper in
// cmd/worker) re-XADDs these. No locking: reclaim is idempotent via the
// ClaimRunning status guard.
func (s *JobsStore) ListStalePending(ctx context.Context, olderThan time.Time, limit int) ([]task.Job, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT `+jobColumns+` FROM jobs
		WHERE status IN ('pending', 'queued') AND created_at < $1
		ORDER BY created_at LIMIT $2`, olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("list stale pending: %w", err)
	}
	defer rows.Close()

	var out []task.Job
	for rows.Next() {
		var job task.Job
		if err := scanJobRow(&job, rows); err != nil {
			return nil, fmt.Errorf("scan stale job: %w", err)
		}
		hydratePayload(&job)
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows stale pending: %w", err)
	}
	return out, nil
}
