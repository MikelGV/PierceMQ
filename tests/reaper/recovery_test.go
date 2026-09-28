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
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestRecoveryAfterRedisLoss is the §11.5 application-side recovery test:
// when the delivery buffer loses everything (failover loss, restart without
// persistence), jobs durable in PostgreSQL are re-enqueued from DB state and
// become claimable again — no job is lost, at-most a re-delivery the claim
// guard absorbs.
func TestRecoveryAfterRedisLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Postgres: the source of truth.
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
	pgHost, err := pgC.Host(ctx)
	require.NoError(t, err)
	pgPort, err := pgC.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://admin:admin@%s:%s/piercemq?sslmode=disable", pgHost, pgPort.Port())
	require.NoError(t, storage.MigrateUp(ctx, dsn))
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := jobs.New(db, db)

	// Redis with a handle, so the test can kill and revive it.
	redisC, err := testcontainers.Run(ctx, "redis:latest",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
			wait.ForLog("Ready to accept connections"),
		),
	)
	testcontainers.CleanupContainer(t, redisC)
	require.NoError(t, err)
	redisURL := func() string {
		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379/tcp")
		require.NoError(t, err)
		return fmt.Sprintf("redis://%s:%s", host, port.Port())
	}
	rds, err := broker.Redis_Connect(redisURL())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rds.Conn.Close() })

	// A pending job that never reached the stream — the post-loss steady
	// state for every in-flight job.
	created, err := store.CreateJob(ctx, task.Job{
		Type:           "email",
		QueueName:      "email-high",
		Priority:       1,
		MaxRetry:       3,
		IdempotencyKey: sql.NullString{String: "r-loss-1", Valid: true},
		CreatedAt:      time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC),
		Payload:        map[string]any{"to": "a@example.com"},
	})
	require.NoError(t, err)

	// Kill and revive Redis: the buffer loses everything. The mapped port
	// changes across a restart, so re-resolve and reconnect (production
	// compose pins fixed ports; only the test harness remaps).
	tenSec := 10 * time.Second
	require.NoError(t, redisC.Stop(ctx, &tenSec))
	require.NoError(t, redisC.Start(ctx))
	require.NoError(t, rds.Conn.Close())
	rds, err = broker.Redis_Connect(redisURL())
	require.NoError(t, err)

	// The same client reconnects transparently (ServeJobs backoff path).
	require.Eventually(t, func() bool {
		return rds.Conn.Ping(ctx).Err() == nil
	}, 60*time.Second, 500*time.Millisecond, "client must reconnect after restart")

	// The reaper sweep re-enqueues the orphan from PostgreSQL state.
	r := reaper.New(store, rds, time.Second, 10, 90*time.Second, time.Minute)
	stats, err := r.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Swept)

	n, err := rds.Conn.XLen(ctx, queue.EmailHighStream).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// And a healthy worker can claim it.
	claimed, _, err := store.ClaimRunning(ctx, created.JobID, "worker-after-loss")
	require.NoError(t, err)
	require.Equal(t, task.JobRunning, claimed.Status)
}
