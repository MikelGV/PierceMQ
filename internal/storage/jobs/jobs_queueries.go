package jobs

import (
	"context"
	"database/sql"
	"fmt"
)

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

func (j *JobsStore) CreateJob(ctx context.Context, jobs string) error {
	trans, err := j.write.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("Error couldn't start transaction: %w", err)
	}

	defer trans.Rollback()
	query := `INSERT INTO jobs (
		type,
		payload_ref,
		queue_name,
		idempontecy_key,
		scheduled_at,
		priority,
		max_retries
	) VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (idempontecy_key, created_at) WHERE idempontecy_key IS NOT NULL DO NOTHING
	RETURNING job_id, status, created_at, attempt_count, max_retries, priority;`

	err = trans.QueryRowContext(ctx, query, jobs).
		Scan()

	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w", err)
		}
		return fmt.Errorf("insert job: %w", err)
	}

	q := `INSERT INTO job_events () VALUES ()`

	_, err = trans.ExecContext(ctx, q, jobs)
	if err != nil {
		return fmt.Errorf("insert job_event: %w", err)
	}

	if err := trans.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
