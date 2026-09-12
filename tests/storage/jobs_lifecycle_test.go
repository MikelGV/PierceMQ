package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClaimRunning(t *testing.T) {
	ctx := context.Background()

	t.Run("claim moves pending to running with token and event", func(t *testing.T) {
		store, db := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-claim-1", sept2026()))
		require.NoError(t, err)

		claimed, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, token)
		require.Equal(t, task.JobRunning, claimed.Status)
		require.Equal(t, int16(1), claimed.AttemptCount)
		require.NotNil(t, claimed.Payload)
		require.Equal(t, 2, countEvents(t, db, created.JobID.String()))
	})

	t.Run("double claim returns ErrNotClaimable", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-claim-2", sept2026()))
		require.NoError(t, err)

		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-2")
		require.ErrorIs(t, err, jobs.ErrNotClaimable)
	})

	t.Run("claim missing job returns ErrNotClaimable", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		_, _, err := store.ClaimRunning(ctx, uuid.New(), "worker-1")
		require.ErrorIs(t, err, jobs.ErrNotClaimable)
	})
}

func TestHeartbeat(t *testing.T) {
	ctx := context.Background()

	t.Run("heartbeat refreshes running claim", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-hb-1", sept2026()))
		require.NoError(t, err)

		_, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		require.NoError(t, store.Heartbeat(ctx, created.JobID, token))
	})

	t.Run("heartbeat with wrong token returns ErrNotClaimable", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-hb-2", sept2026()))
		require.NoError(t, err)

		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		require.ErrorIs(t, store.Heartbeat(ctx, created.JobID, uuid.New()), jobs.ErrNotClaimable)
	})
}

func TestComplete(t *testing.T) {
	ctx := context.Background()

	t.Run("complete moves running to completed with event", func(t *testing.T) {
		store, db := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-done-1", sept2026()))
		require.NoError(t, err)

		_, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		require.NoError(t, store.Complete(ctx, created.JobID, token))
		require.Equal(t, 3, countEvents(t, db, created.JobID.String()))

		// Terminal rows cannot be re-claimed: redeliveries are no-ops.
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-2")
		require.ErrorIs(t, err, jobs.ErrNotClaimable)
	})

	t.Run("complete with wrong token returns ErrNotClaimable", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-done-2", sept2026()))
		require.NoError(t, err)

		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		require.ErrorIs(t, store.Complete(ctx, created.JobID, uuid.New()), jobs.ErrNotClaimable)
	})
}

func TestFailOrRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("failure below max returns to pending", func(t *testing.T) {
		store, db := setupJobsStore(t)

		in := baseJob("k-fail-1", sept2026())
		in.MaxRetry = 3
		created, err := store.CreateJob(ctx, in)
		require.NoError(t, err)

		_, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		out, err := store.FailOrRetry(ctx, created.JobID, token, "boom")
		require.NoError(t, err)
		require.Equal(t, task.JobPending, out.Status)
		require.Equal(t, "boom", out.LastError.String)
		require.Equal(t, 3, countEvents(t, db, created.JobID.String()))

		// Back to pending means it can be claimed again.
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-2")
		require.NoError(t, err)
	})

	t.Run("failure at max moves to failed", func(t *testing.T) {
		store, db := setupJobsStore(t)

		in := baseJob("k-fail-2", sept2026())
		in.MaxRetry = 1
		created, err := store.CreateJob(ctx, in)
		require.NoError(t, err)

		_, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
		require.NoError(t, err)

		out, err := store.FailOrRetry(ctx, created.JobID, token, "fatal")
		require.NoError(t, err)
		require.Equal(t, task.JobFailed, out.Status)
		require.Equal(t, 3, countEvents(t, db, created.JobID.String()))

		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-2")
		require.ErrorIs(t, err, jobs.ErrNotClaimable)
	})
}

func TestListStalePending(t *testing.T) {
	ctx := context.Background()

	t.Run("returns only stale pending rows", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		old := baseJob("k-stale-1", sept2026())
		_, err := store.CreateJob(ctx, old)
		require.NoError(t, err)

		fresh := baseJob("k-stale-2", sept2026().Add(48*time.Hour))
		_, err = store.CreateJob(ctx, fresh)
		require.NoError(t, err)

		stale, err := store.ListStalePending(ctx, sept2026().Add(24*time.Hour), 50)
		require.NoError(t, err)
		require.Len(t, stale, 1)
		require.Equal(t, "k-stale-1", stale[0].IdempotencyKey.String)
	})
}
