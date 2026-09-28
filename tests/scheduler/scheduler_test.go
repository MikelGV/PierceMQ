package scheduler_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/scheduler"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupScheduler(t *testing.T) (*scheduler.Scheduler, *jobs.JobsStore, *broker.RedisStore) {
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
	return scheduler.New(store, rds, time.Second, 10), store, rds
}

func scheduledEmail(key, queueName string, priority int16, at time.Time) task.Job {
	return task.Job{
		Type:           "email",
		QueueName:      queueName,
		Priority:       priority,
		MaxRetry:       3,
		Status:         task.JobScheduled,
		ScheduledAt:    sql.NullTime{Time: at, Valid: true},
		CreatedAt:      time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: sql.NullString{String: key, Valid: true},
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
	past := time.Now().UTC().Add(-time.Hour)

	t.Run("promotes due jobs to their streams, leaves future alone", func(t *testing.T) {
		sched, store, rds := setupScheduler(t)

		dueHigh, err := store.CreateJob(ctx, scheduledEmail("s-tick-1", "email-high", 1, past))
		require.NoError(t, err)
		_, err = store.CreateJob(ctx, scheduledEmail("s-tick-2", "email-low", 0, past))
		require.NoError(t, err)
		future, err := store.CreateJob(ctx, scheduledEmail("s-tick-3", "email-high", 1, time.Now().UTC().Add(24*time.Hour)))
		require.NoError(t, err)

		n, err := sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := store.GetJobByID(ctx, dueHigh.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)

		still, err := store.GetJobByID(ctx, future.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobScheduled, still.Status)

		require.Equal(t, 1, streamLen(t, rds, queue.EmailHighStream))
		require.Equal(t, 1, streamLen(t, rds, queue.EmailLowStream))
	})

	t.Run("nothing due dispatches nothing", func(t *testing.T) {
		sched, _, _ := setupScheduler(t)

		n, err := sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("unknown queue stays pending without failing the tick", func(t *testing.T) {
		sched, store, _ := setupScheduler(t)

		bad := scheduledEmail("s-tick-4", "nope-high", 1, past)
		created, err := store.CreateJob(ctx, bad)
		require.NoError(t, err)

		n, err := sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		got, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)
	})
}
