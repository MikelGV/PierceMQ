package storage_test

import (
	"context"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// failJob drives a job to failed: claim then exhaust retries.
func failJob(t *testing.T, store *jobs.JobsStore, ctx context.Context, key string, maxRetry int16) task.Job {
	t.Helper()
	in := baseJob(key, sept2026())
	in.MaxRetry = maxRetry
	created, err := store.CreateJob(ctx, in)
	require.NoError(t, err)
	_, token, err := store.ClaimRunning(ctx, created.JobID, "worker-1")
	require.NoError(t, err)
	out, err := store.FailOrRetry(ctx, created.JobID, token, "boom")
	require.NoError(t, err)
	require.Equal(t, task.JobFailed, out.Status)
	return out
}

func TestRetryJob(t *testing.T) {
	ctx := context.Background()

	t.Run("failed job resets to pending with zeroed attempts and event", func(t *testing.T) {
		store, db := setupJobsStore(t)
		failed := failJob(t, store, ctx, "k-retry-1", 1)

		out, err := store.RetryJob(ctx, failed.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, out.Status)
		require.Equal(t, int16(0), out.AttemptCount)
		require.False(t, out.LastError.Valid, "last error cleared")
		require.False(t, out.ClaimToken.Valid, "claim cleared")
		require.Equal(t, 4, countEvents(t, db, failed.JobID.String()))

		// Retried jobs are claimable again.
		_, _, err = store.ClaimRunning(ctx, failed.JobID, "worker-2")
		require.NoError(t, err)
	})

	t.Run("non-failed jobs are not retryable", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		pending, err := store.CreateJob(ctx, baseJob("k-retry-2", sept2026()))
		require.NoError(t, err)
		_, err = store.RetryJob(ctx, pending.JobID)
		require.ErrorIs(t, err, jobs.ErrNotRetryable)

		_, _, err = store.ClaimRunning(ctx, pending.JobID, "worker-1")
		require.NoError(t, err)
		_, err = store.RetryJob(ctx, pending.JobID)
		require.ErrorIs(t, err, jobs.ErrNotRetryable)
	})

	t.Run("missing job is not retryable", func(t *testing.T) {
		store, _ := setupJobsStore(t)
		_, err := store.RetryJob(ctx, uuid.New())
		require.ErrorIs(t, err, jobs.ErrNotRetryable)
	})
}

func TestCancelJob(t *testing.T) {
	ctx := context.Background()

	t.Run("pending and scheduled jobs cancel with event", func(t *testing.T) {
		store, db := setupJobsStore(t)

		pending, err := store.CreateJob(ctx, baseJob("k-cancel-1", sept2026()))
		require.NoError(t, err)
		out, err := store.CancelJob(ctx, pending.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobCancelled, out.Status)
		require.True(t, out.CompletedAt.Valid, "terminal stamp set")
		require.Equal(t, 2, countEvents(t, db, pending.JobID.String()))

		// Cancelled jobs can never be claimed: orphaned stream entries ack+skip.
		_, _, err = store.ClaimRunning(ctx, pending.JobID, "worker-1")
		require.ErrorIs(t, err, jobs.ErrNotClaimable)
	})

	t.Run("running completed and failed jobs cannot cancel", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		running, err := store.CreateJob(ctx, baseJob("k-cancel-2", sept2026()))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, running.JobID, "worker-1")
		require.NoError(t, err)
		_, err = store.CancelJob(ctx, running.JobID)
		require.ErrorIs(t, err, jobs.ErrNotCancellable)

		failed := failJob(t, store, ctx, "k-cancel-3", 1)
		_, err = store.CancelJob(ctx, failed.JobID)
		require.ErrorIs(t, err, jobs.ErrNotCancellable)
	})

	t.Run("missing job cannot cancel", func(t *testing.T) {
		store, _ := setupJobsStore(t)
		_, err := store.CancelJob(ctx, uuid.New())
		require.ErrorIs(t, err, jobs.ErrNotCancellable)
	})
}

func TestListJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("lists newest first with status filter and pagination", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		for _, k := range []string{"k-list-1", "k-list-2", "k-list-3"} {
			_, err := store.CreateJob(ctx, baseJob(k, sept2026()))
			require.NoError(t, err)
		}
		failed := failJob(t, store, ctx, "k-list-4", 1)
		require.Equal(t, task.JobFailed, failed.Status)

		all, err := store.ListJobs(ctx, "", 50, 0)
		require.NoError(t, err)
		require.Len(t, all, 4)
		// Newest first.
		require.True(t, !all[0].CreatedAt.Before(all[1].CreatedAt))

		onlyFailed, err := store.ListJobs(ctx, task.JobFailed, 50, 0)
		require.NoError(t, err)
		require.Len(t, onlyFailed, 1)
		require.Equal(t, failed.JobID, onlyFailed[0].JobID)

		page1, err := store.ListJobs(ctx, "", 2, 0)
		require.NoError(t, err)
		require.Len(t, page1, 2)
		page2, err := store.ListJobs(ctx, "", 2, 2)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		require.NotEqual(t, page1[0].JobID, page2[0].JobID)
	})
}

func TestJobStats(t *testing.T) {
	ctx := context.Background()

	t.Run("counts group by status", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		_, err := store.CreateJob(ctx, baseJob("k-stats-1", sept2026()))
		require.NoError(t, err)
		_, err = store.CreateJob(ctx, baseJob("k-stats-2", sept2026()))
		require.NoError(t, err)
		failed := failJob(t, store, ctx, "k-stats-3", 1)
		require.Equal(t, task.JobFailed, failed.Status)

		running, err := store.CreateJob(ctx, baseJob("k-stats-4", sept2026()))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, running.JobID, "worker-1")
		require.NoError(t, err)

		stats, err := store.JobStats(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), stats.Pending)
		require.Equal(t, int64(1), stats.Running)
		require.Equal(t, int64(1), stats.Failed)
		require.Equal(t, int64(0), stats.Completed)
	})
}
