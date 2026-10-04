package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// nullableOwner maps the optional owner scoping arg to a nullable query param:
// no owner (system-wide internal reads) binds NULL which disables the filter.
func nullableOwner(owner []uuid.UUID) any {
	if len(owner) > 0 && owner[0] != uuid.Nil {
		return owner[0]
	}
	return nil
}

// ErrRetryKeyConflict is returned when a retry Idempotency-Key was already
// consumed by a different job. Replays of the same key+job are idempotent
// (see RecordRetryKey). Detect via errors.Is.
var ErrRetryKeyConflict = errors.New("jobs: retry idempotency key already used")

// RecordRetryKey consumes a retry idempotency key for jobID (§9.6: the
// Idempotency-Key header makes POST .../retry safe to resend). First use
// records (key -> jobID) and reports isDup=false. A replay of the same
// key+job reports isDup=true with no error: the caller returns the job's
// current state without re-dispatching. A key bound to another job reports
// ErrRetryKeyConflict. Empty keys are no-ops (isDup=false, nil error).
//
// Fresh-vs-replay comes from RowsAffected of a single INSERT ... ON
// CONFLICT DO NOTHING, so concurrent replays are race-free.
func (s *JobsStore) RecordRetryKey(ctx context.Context, key string, jobID uuid.UUID) (isDup bool, err error) {
	if key == "" {
		return false, nil
	}
	res, err := s.write.ExecContext(ctx,
		`INSERT INTO retry_idempotency (idempotency_key, job_id) VALUES ($1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING`, key, jobID)
	if err != nil {
		return false, fmt.Errorf("record retry key: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record retry key rows: %w", err)
	}
	if n == 1 {
		return false, nil
	}
	var owner uuid.UUID
	if err := s.write.QueryRowContext(ctx,
		`SELECT job_id FROM retry_idempotency WHERE idempotency_key = $1`, key,
	).Scan(&owner); err != nil {
		return false, fmt.Errorf("read retry key: %w", err)
	}
	if owner != jobID {
		return false, ErrRetryKeyConflict
	}
	return true, nil
}

// ErrNotRetryable is returned when a job cannot be retried: it is not in
// failed status, or the row is missing. Detect via errors.Is.
var ErrNotRetryable = errors.New("jobs: not retryable")

// ErrNotCancellable is returned when a job cannot be cancelled: it already
// started running or is terminal, or the row is missing. Detect via
// errors.Is.
var ErrNotCancellable = errors.New("jobs: not cancellable")

// RetryJob resets a failed job to pending with a zeroed attempt count, per
// §12.3: manual inspection followed by replay via POST /jobs/{id}/retry. The
// caller re-XADDs the returned job; on dispatch failure it stays pending and
// the reaper sweep picks it up. Appends a failed→pending job_events row.
func (s *JobsStore) RetryJob(ctx context.Context, jobID uuid.UUID) (task.Job, error) {
	var out task.Job

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status task.JobStatus
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = $1 FOR UPDATE`, jobID,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, ErrNotRetryable
		}
		return out, fmt.Errorf("select job for retry: %w", err)
	}
	if status != task.JobFailed {
		return out, ErrNotRetryable
	}

	if err := scanJobRow(&out, tx.QueryRowContext(ctx,
		`UPDATE jobs SET status = 'pending', attempt_count = 0,
			claim_token = NULL, worker_id = NULL, last_error = NULL,
			not_before = NULL, completed_at = NULL
		WHERE job_id = $1 RETURNING `+jobColumns, jobID)); err != nil {
		return out, fmt.Errorf("retry job: %w", err)
	}
	if err := insertEvent(ctx, tx, jobID, task.JobFailed, task.JobPending); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("commit: %w", err)
	}

	hydratePayload(&out)
	return out, nil
}

// CancelJob moves a not-yet-running job (scheduled, pending, or queued) to
// cancelled with a terminal stamp, per §9.3. Running and terminal jobs
// cannot be cancelled. Appends an old→cancelled job_events row.
//
// The orphaned stream entry (if the job was already XADDed) is intentionally
// left in place: when a worker receives it, ClaimRunning rejects it as not
// claimable and the entry is acked + skipped — no separate delete needed.
func (s *JobsStore) CancelJob(ctx context.Context, jobID uuid.UUID) (task.Job, error) {
	var out task.Job

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status task.JobStatus
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = $1 FOR UPDATE`, jobID,
	).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, ErrNotCancellable
		}
		return out, fmt.Errorf("select job for cancel: %w", err)
	}
	if status != task.JobScheduled && status != task.JobPending && status != task.JobQueued {
		return out, ErrNotCancellable
	}

	if err := scanJobRow(&out, tx.QueryRowContext(ctx,
		`UPDATE jobs SET status = 'cancelled', completed_at = now()
		WHERE job_id = $1 RETURNING `+jobColumns, jobID)); err != nil {
		return out, fmt.Errorf("cancel job: %w", err)
	}
	if err := insertEvent(ctx, tx, jobID, status, task.JobCancelled); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("commit: %w", err)
	}

	hydratePayload(&out)
	return out, nil
}

// ListSeriesJobs returns members of a recurring series newest-first,
// optionally filtered by status. Owner scoping matches ListJobs: callers
// passing a user_id see only their rows.
func (s *JobsStore) ListSeriesJobs(ctx context.Context, seriesID uuid.UUID, status task.JobStatus, limit, offset int, owner ...uuid.UUID) ([]task.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := s.read.QueryContext(ctx,
		`SELECT `+jobColumns+` FROM jobs
		WHERE series_id = $1
			AND ($2 = '' OR status = $2::job_status)
			AND ($5::UUID IS NULL OR owner_user_id = $5)
		ORDER BY created_at DESC LIMIT $3 OFFSET $4`,
		seriesID, string(status), limit, offset, nullableOwner(owner))
	if err != nil {
		return nil, fmt.Errorf("list series jobs: %w", err)
	}
	defer rows.Close()

	var out []task.Job
	for rows.Next() {
		var job task.Job
		if err := scanJobRow(&job, rows); err != nil {
			return nil, fmt.Errorf("scan series job: %w", err)
		}
		hydratePayload(&job)
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows series jobs: %w", err)
	}
	return out, nil
}

// CancelSeries cancels every not-yet-running member (scheduled, pending, or
// queued) of a recurring series. Running and terminal members are left
// alone, matching single CancelJob semantics; future occurrences stop
// because promotion only fires on scheduled rows. Each cancellation appends
// an old→cancelled job_events row. Returns the number cancelled.
//
// Concurrency: SELECT ... FOR UPDATE SKIP LOCKED, so concurrent cancellers
// never double-cancel; the status-guarded UPDATE skips lost races.
func (s *JobsStore) CancelSeries(ctx context.Context, seriesID uuid.UUID, owner ...uuid.UUID) (int64, error) {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT job_id, status FROM jobs
		WHERE series_id = $1 AND status IN ('scheduled', 'pending', 'queued')
			AND ($2::UUID IS NULL OR owner_user_id = $2)
		ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`,
		seriesID, nullableOwner(owner))
	if err != nil {
		return 0, fmt.Errorf("select series jobs: %w", err)
	}
	type cancellable struct {
		id     uuid.UUID
		status task.JobStatus
	}
	var targets []cancellable
	for rows.Next() {
		var c cancellable
		if err := rows.Scan(&c.id, &c.status); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan series job: %w", err)
		}
		targets = append(targets, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("rows series jobs: %w", err)
	}
	rows.Close()

	var n int64
	for _, c := range targets {
		res, err := tx.ExecContext(ctx,
			`UPDATE jobs SET status = 'cancelled', completed_at = now()
			WHERE job_id = $1 AND status IN ('scheduled', 'pending', 'queued')`, c.id)
		if err != nil {
			return 0, fmt.Errorf("cancel series job %s: %w", c.id, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("cancel series job rows %s: %w", c.id, err)
		}
		if affected == 0 {
			continue
		}
		if err := insertEvent(ctx, tx, c.id, c.status, task.JobCancelled); err != nil {
			return 0, err
		}
		n++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}

// ListJobs returns jobs newest-first, optionally filtered by status
// (§9.4-9.5). Empty status lists all. Limit is clamped to 1..100; offset must
// be >= 0. Served from the read pool: a few seconds of lag is accepted for
// observability reads (§11.3.1).
//
// Optional owner scoping (tenant isolation): when a caller user_id is passed,
// only that user's rows are listed; legacy NULL-owner rows are excluded.
// Internal callers omit owner for system-wide reads.
func (s *JobsStore) ListJobs(ctx context.Context, status task.JobStatus, limit, offset int, owner ...uuid.UUID) ([]task.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := s.read.QueryContext(ctx,
		`SELECT `+jobColumns+` FROM jobs
		WHERE ($1 = '' OR status = $1::job_status)
			AND ($4::UUID IS NULL OR owner_user_id = $4)
		ORDER BY created_at DESC LIMIT $2 OFFSET $3`, string(status), limit, offset, nullableOwner(owner))
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var out []task.Job
	for rows.Next() {
		var job task.Job
		if err := scanJobRow(&job, rows); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		hydratePayload(&job)
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows jobs: %w", err)
	}
	return out, nil
}

// Stats holds per-status job counts for GET /stats (§9.8).
type Stats struct {
	Scheduled int64
	Pending   int64
	Queued    int64
	Running   int64
	Completed int64
	Failed    int64
	Cancelled int64
}

// OldestPendingAgeSec returns the age in seconds of the oldest
// pending/queued job (0 when none). Serves GET /v1/stats
// pending_oldest_age_sec: a growing value with no corresponding decrease
// signals scheduler/reaper stall (§12.2). Read pool. Optional owner scopes
// to the caller's jobs.
func (s *JobsStore) OldestPendingAgeSec(ctx context.Context, owner ...uuid.UUID) (int64, error) {
	var age sql.NullInt64
	if err := s.read.QueryRowContext(ctx,
		`SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at)))::BIGINT
		FROM jobs WHERE status IN ('pending', 'queued')
			AND ($1::UUID IS NULL OR owner_user_id = $1)`, nullableOwner(owner)).Scan(&age); err != nil {
		return 0, fmt.Errorf("oldest pending age: %w", err)
	}
	if !age.Valid {
		return 0, nil
	}
	return age.Int64, nil
}

// JobStats counts jobs per status in one pass. Runs at repeatable read (§10.4
// snapshot consistency) on the read pool: reporting sees a stable snapshot,
// staleness of seconds is accepted. Optional owner scopes to the caller's jobs.
func (s *JobsStore) JobStats(ctx context.Context, owner ...uuid.UUID) (Stats, error) {
	var stats Stats

	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return stats, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT status, count(*) FROM jobs
		WHERE ($1::UUID IS NULL OR owner_user_id = $1)
		GROUP BY status`, nullableOwner(owner))
	if err != nil {
		return stats, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status task.JobStatus
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return stats, fmt.Errorf("scan stats: %w", err)
		}
		switch status {
		case task.JobScheduled:
			stats.Scheduled = n
		case task.JobPending:
			stats.Pending = n
		case task.JobQueued:
			stats.Queued = n
		case task.JobRunning:
			stats.Running = n
		case task.JobCompleted:
			stats.Completed = n
		case task.JobFailed:
			stats.Failed = n
		case task.JobCancelled:
			stats.Cancelled = n
		}
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("rows stats: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return stats, fmt.Errorf("commit: %w", err)
	}
	return stats, nil
}
