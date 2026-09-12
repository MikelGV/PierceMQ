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

// ErrJobExists is returned when ON CONFLICT DO NOTHING skips the insert
// (same idempotency_key + created_at). Detect via errors.Is.
var ErrJobExists = errors.New("jobs: duplicate idempotency key")

// JobsStore routes internally: writes (+ SELECT ... FOR UPDATE) go to
// write (primary via PgBouncer `piercemq`), plain reads go to read
// (replicas via `piercemq_ro`). Callers never pick a handle.
type JobsStore struct {
	write *sql.DB
	read  *sql.DB
}

// New wires a JobsStore from the lean storage.Stores handles.
func New(write, read *sql.DB) *JobsStore {
	return &JobsStore{write: write, read: read}
}

// CreateJob inserts a job + its initial job_events row atomically.
//
// Idempotency contract: the UNIQUE is (idempotency_key, created_at), so
// callers MUST reuse the same CreatedAt on retry for ON CONFLICT to fire.
// Same key + different CreatedAt lands in another partition and inserts.
// Zero CreatedAt defaults to now (UTC); zero JobID gets a fresh UUID;
// empty Status defaults to pending.
func (s *JobsStore) CreateJob(ctx context.Context, in task.Job) (task.Job, error) {
	var out task.Job
	if in.JobID == uuid.Nil {
		in.JobID = uuid.New()
	}
	if in.Status == "" {
		in.Status = task.JobPending
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now().UTC()
	}
	if in.MaxRetry == 0 {
		in.MaxRetry = 3
	}

	payloadRef := in.PayloadRef
	if !payloadRef.Valid && in.Payload != nil {
		if b, err := json.Marshal(in.Payload); err == nil {
			payloadRef = sql.NullString{String: string(b), Valid: true}
		}
	}

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertJob = `INSERT INTO jobs (
		job_id,
		status,
		type,
		payload_ref,
		queue_name,
		priority,
		attempt_count,
		max_retry,
		idempotency_key,
		scheduled_at,
		created_at
	) VALUES ($1, $2::job_status, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (idempotency_key, created_at) WHERE idempotency_key IS NOT NULL DO NOTHING
	RETURNING job_id, status, created_at, attempt_count, max_retry, priority;`

	err = tx.QueryRowContext(ctx, insertJob,
		in.JobID,
		string(in.Status),
		in.Type,
		payloadRef,
		in.QueueName,
		in.Priority,
		in.AttemptCount,
		in.MaxRetry,
		in.IdempotencyKey,
		in.ScheduledAt,
		in.CreatedAt,
	).Scan(&out.JobID, &out.Status, &out.CreatedAt, &out.AttemptCount, &out.MaxRetry, &out.Priority)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, ErrJobExists
		}
		return out, fmt.Errorf("insert job: %w", err)
	}

	const insertEvent = `INSERT INTO job_events (
		event_id,
		job_id,
		old_status,
		new_status,
		occurred_at
	) VALUES (gen_random_uuid(), $1, NULL, $2::job_status, now());`

	if _, err := tx.ExecContext(ctx, insertEvent, out.JobID, string(out.Status)); err != nil {
		return out, fmt.Errorf("insert job_event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("commit: %w", err)
	}

	out.Type = in.Type
	out.PayloadRef = payloadRef
	out.Payload = in.Payload
	out.QueueName = in.QueueName
	out.IdempotencyKey = in.IdempotencyKey
	out.ScheduledAt = in.ScheduledAt
	return out, nil
}
