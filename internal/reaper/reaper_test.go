package reaper_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/reaper"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupReaper(t *testing.T) (*reaper.Reaper, *jobs.JobsStore, *broker.RedisStore, *sql.DB) {
	t.Helper()
	ctx := context.Background()

	pgC, err := testcontainers.Run(ctx, "postgres:17-alpine",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "admin",
			"POSTGRES_PASSWORD": "admin",
			"POSTGRES_DB":       "piercemq",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
			wait.ForLog("database system is ready to accept connections"),
		),
	)
	testcontainers.CleanupContainer(t, pgC)
	require.NoError(t, err)

	host, err := pgC.Host(ctx)
	require.NoError(t, err)
	port, err := pgC.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://admin:admin@%s:%s/piercemq?sslmode=disable", host, port.Port())
	require.NoError(t, storage.MigrateUp(ctx, dsn))

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rds := utils_test.SetUpRedis(t)
	store := jobs.New(db, db)
	// poll 1s (unused by Tick), batch 10, stale-after 90s, sweep pending older than 5m.
	r := reaper.New(store, rds, time.Second, 10, 90*time.Second, 5*time.Minute)
	return r, store, rds, db
}

func emailJob(key string, createdAt time.Time) task.Job {
	return task.Job{
		Type:           "email",
		QueueName:      "email-high",
		Priority:       1,
		MaxRetry:       3,
		IdempotencyKey: sql.NullString{String: key, Valid: true},
		CreatedAt:      createdAt,
		Payload:        map[string]any{"to": "a@example.com"},
	}
}

func streamLen(t *testing.T, rds *broker.RedisStore, stream string) int {
	t.Helper()
	n, err := rds.Conn.XLen(context.Background(), stream).Result()
	require.NoError(t, err)
	return int(n)
}

func TestTick(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	// Inside the Sep 2026 partition, older than the 5m sweep cutoff.
	old := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	t.Run("reclaims crashed worker jobs to pending and re-enqueues them", func(t *testing.T) {
		r, store, rds, db := setupReaper(t)

		created, err := store.CreateJob(ctx, emailJob("r-tick-1", old))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-dead")
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE jobs SET heartbeat_at = $1 WHERE job_id = $2`,
			now.Add(-5*time.Minute), created.JobID.String())
		require.NoError(t, err)

		stats, err := r.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.Reclaimed)
		require.Equal(t, 1, stats.Reenqueued)

		got, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)
		require.Equal(t, 1, streamLen(t, rds, queue.EmailHighStream))
	})

	t.Run("live workers are left alone", func(t *testing.T) {
		r, store, rds, _ := setupReaper(t)

		created, err := store.CreateJob(ctx, emailJob("r-tick-2", old))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-alive")
		require.NoError(t, err)

		stats, err := r.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, stats.Reclaimed)

		got, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobRunning, got.Status)
		require.Equal(t, 0, streamLen(t, rds, queue.EmailHighStream))
	})

	t.Run("sweeps stale pending jobs that never reached the stream", func(t *testing.T) {
		r, store, rds, _ := setupReaper(t)

		created, err := store.CreateJob(ctx, emailJob("r-tick-3", old))
		require.NoError(t, err)

		stats, err := r.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, stats.Reclaimed)
		require.Equal(t, 1, stats.Swept)

		got, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)
		require.Equal(t, 1, streamLen(t, rds, queue.EmailHighStream))
	})

	t.Run("fresh pending jobs are not swept", func(t *testing.T) {
		r, store, rds, _ := setupReaper(t)

		freshAt := time.Now().UTC()
		created, err := store.CreateJob(ctx, emailJob("r-tick-4", freshAt))
		require.NoError(t, err)

		stats, err := r.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, stats.Swept)

		got, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)
		require.Equal(t, 0, streamLen(t, rds, queue.EmailHighStream))
	})
}
