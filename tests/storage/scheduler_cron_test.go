package storage_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// recurringJob builds a due series member: scheduled in the past with a
// per-minute cadence.
func recurringJob(key string, seriesID uuid.UUID) task.Job {
	j := baseJob(key, sept2026())
	j.Status = task.JobScheduled
	j.ScheduledAt = sql.NullTime{Time: dueAt(), Valid: true}
	j.IsRecurring = true
	j.CronExpr = sql.NullString{String: "* * * * *", Valid: true}
	j.CronTZ = sql.NullString{String: "UTC", Valid: true}
	j.SeriesID = uuid.NullUUID{UUID: seriesID, Valid: true}
	return j
}

func TestClaimDueScheduledRecurrence(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("promoting a series member queues exactly one follow-up", func(t *testing.T) {
		store, db := setupJobsStore(t)
		series := uuid.New()

		parent, err := store.CreateJob(ctx, recurringJob("k-cron-1", series))
		require.NoError(t, err)

		promoted, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Len(t, promoted, 1)
		require.Equal(t, task.JobPending, promoted[0].Status)

		members, err := store.ListSeriesJobs(ctx, series, "", 50, 0)
		require.NoError(t, err)
		require.Len(t, members, 2, "parent + one follow-up")

		var child task.Job
		for _, m := range members {
			if m.JobID != parent.JobID {
				child = m
			}
		}
		require.Equal(t, task.JobScheduled, child.Status)
		require.True(t, child.IsRecurring)
		require.Equal(t, series, child.SeriesID.UUID)
		require.Equal(t, parent.Type, child.Type)
		require.Equal(t, parent.QueueName, child.QueueName)
		require.Equal(t, parent.MaxRetry, child.MaxRetry)
		require.False(t, child.IdempotencyKey.Valid, "each occurrence is a fresh job")
		require.True(t, child.ScheduledAt.Time.After(now.Add(-time.Minute)),
			"follow-up fires after the claim, got %v", child.ScheduledAt.Time)
		require.Equal(t, 1, countEvents(t, db, child.JobID.String()))

		// Second claim at the same instant finds nothing: the child is future.
		again, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Empty(t, again)
	})

	t.Run("one-shot jobs spawn no follow-ups", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		_, err := store.CreateJob(ctx, scheduledJob("k-cron-2", dueAt()))
		require.NoError(t, err)

		promoted, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Len(t, promoted, 1)
		require.False(t, promoted[0].IsRecurring)
	})

	t.Run("poisoned expression still promotes, series ends", func(t *testing.T) {
		store, _ := setupJobsStore(t)
		series := uuid.New()

		bad := recurringJob("k-cron-3", series)
		bad.CronExpr = sql.NullString{String: "not a cron", Valid: true}
		created, err := store.CreateJob(ctx, bad)
		require.NoError(t, err)

		promoted, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Len(t, promoted, 1)
		require.Equal(t, created.JobID, promoted[0].JobID)

		members, err := store.ListSeriesJobs(ctx, series, "", 50, 0)
		require.NoError(t, err)
		require.Len(t, members, 1, "no follow-up for invalid expression")
	})
}

func TestCancelSeries(t *testing.T) {
	ctx := context.Background()

	t.Run("cancels not-yet-running members, leaves running alone", func(t *testing.T) {
		store, db := setupJobsStore(t)
		series := uuid.New()

		sched, err := store.CreateJob(ctx, recurringJob("k-cs-1", series))
		require.NoError(t, err)
		pend, err := store.CreateJob(ctx, recurringJob("k-cs-2", series))
		require.NoError(t, err)
		// pend is scheduled; make it pending via claim path.
		promoted, err := store.ClaimDueScheduled(ctx, time.Now().UTC(), 10)
		require.NoError(t, err)
		require.Len(t, promoted, 2) // sched + pend; each also queues a follow-up

		running, err := store.CreateJob(ctx, recurringJob("k-cs-3", series))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, running.JobID, "w1")
		require.NoError(t, err)

		n, err := store.CancelSeries(ctx, series)
		require.NoError(t, err)
		// sched(pending) + pend(pending) + 2 follow-ups(scheduled) cancelled;
		// the running member survives.
		require.Equal(t, int64(4), n)

		got, err := store.GetJobByID(ctx, sched.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobCancelled, got.Status)

		got, err = store.GetJobByID(ctx, pend.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobCancelled, got.Status)

		still, err := store.GetJobByID(ctx, running.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobRunning, still.Status)

		require.Equal(t, 3, countEvents(t, db, sched.JobID.String()))
	})

	t.Run("empty series cancels nothing", func(t *testing.T) {
		store, _ := setupJobsStore(t)
		n, err := store.CancelSeries(ctx, uuid.New())
		require.NoError(t, err)
		require.Equal(t, int64(0), n)
	})
}
