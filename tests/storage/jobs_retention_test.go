package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/stretchr/testify/require"
)

func TestDeleteOldJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("purges old completed jobs but keeps failed ones", func(t *testing.T) {
		store, db := setupJobsStore(t)

		completed, err := store.CreateJob(ctx, baseJob("k-ret-1", sept2026()))
		require.NoError(t, err)
		_, token, err := store.ClaimRunning(ctx, completed.JobID, "worker-1")
		require.NoError(t, err)
		require.NoError(t, store.Complete(ctx, completed.JobID, token))

		failed := failJob(t, store, ctx, "k-ret-2", 1)

		old := sept2026().Add(-10 * 24 * time.Hour)
		_, err = db.Exec(`UPDATE jobs SET completed_at = $1 WHERE job_id = $2`, old, completed.JobID.String())
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE jobs SET completed_at = $1 WHERE job_id = $2`, old, failed.JobID.String())
		require.NoError(t, err)

		n, err := store.DeleteOldJobs(ctx, sept2026(), 100)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		_, err = store.GetJobByID(ctx, completed.JobID)
		require.Error(t, err, "purged job must be gone")

		got, err := store.GetJobByID(ctx, failed.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobFailed, got.Status, "failed jobs stay in the DLQ")
	})

	t.Run("empty when nothing is past retention", func(t *testing.T) {
		store, _ := setupJobsStore(t)
		n, err := store.DeleteOldJobs(ctx, sept2026(), 100)
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})
}

func TestEnsureMonthlyPartitions(t *testing.T) {
	ctx := context.Background()
	store, db := setupJobsStore(t)

	future := time.Date(2028, 3, 15, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.EnsureMonthlyPartitions(ctx, future, 1))

	for _, tbl := range []string{"jobs_p2028_03", "jobs_p2028_04", "job_events_p2028_03", "job_events_p2028_04"} {
		var exists bool
		err := db.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)`, tbl).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "partition %s must exist", tbl)
	}

	require.NoError(t, store.EnsureMonthlyPartitions(ctx, future, 1), "idempotent re-run")
}
