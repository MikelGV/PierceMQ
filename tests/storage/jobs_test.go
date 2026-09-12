package storage_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/stretchr/testify/require"
)

// Fixed timestamp inside the Sep 2026 monthly partition (migrations/000004)
// so results are deterministic and never depend on wall-clock month.
func sept2026() time.Time {
	return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
}

func setupJobsStore(t *testing.T) (*jobs.JobsStore, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	dsn := setupPostgres(t)
	require.NoError(t, storage.MigrateUp(ctx, dsn))

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return jobs.New(db, db), db
}

func baseJob(key string, createdAt time.Time) task.Job {
	return task.Job{
		Type:           "email",
		QueueName:      "email-high",
		Priority:       1,
		MaxRetry:       3,
		IdempotencyKey: sql.NullString{String: key, Valid: key != ""},
		CreatedAt:      createdAt,
		Payload:        map[string]any{"to": "a@example.com"},
	}
}

func countEvents(t *testing.T, db *sql.DB, jobID string) int {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT count(*) FROM job_events WHERE job_id = $1`, jobID).Scan(&n)
	require.NoError(t, err)
	return n
}

func TestCreateJob(t *testing.T) {
	ctx := context.Background()

	t.Run("insert ok writes job and initial event", func(t *testing.T) {
		store, db := setupJobsStore(t)

		out, err := store.CreateJob(ctx, baseJob("k-ok-1", sept2026()))
		require.NoError(t, err)
		require.NotEqual(t, "00000000-0000-0000-0000-000000000000", out.JobID.String())
		require.Equal(t, task.JobPending, out.Status)
		require.True(t, out.CreatedAt.Equal(sept2026()))
		require.Equal(t, 1, countEvents(t, db, out.JobID.String()))
	})

	t.Run("duplicate key plus same created_at returns ErrJobExists", func(t *testing.T) {
		store, db := setupJobsStore(t)

		first, err := store.CreateJob(ctx, baseJob("k-dup-1", sept2026()))
		require.NoError(t, err)

		_, err = store.CreateJob(ctx, baseJob("k-dup-1", sept2026()))
		require.ErrorIs(t, err, jobs.ErrJobExists)
		require.Equal(t, 1, countEvents(t, db, first.JobID.String()))
	})

	t.Run("same key plus different created_at inserts", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		_, err := store.CreateJob(ctx, baseJob("k-scope-1", sept2026()))
		require.NoError(t, err)

		other := baseJob("k-scope-1", sept2026().Add(2*time.Hour))
		_, err = store.CreateJob(ctx, other)
		require.NoError(t, err, "different partition means no conflict by design")
	})

	t.Run("null key always inserts", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		_, err := store.CreateJob(ctx, baseJob("", sept2026()))
		require.NoError(t, err)

		_, err = store.CreateJob(ctx, baseJob("", sept2026()))
		require.NoError(t, err, "partial index skips NULL keys")
	})
}
