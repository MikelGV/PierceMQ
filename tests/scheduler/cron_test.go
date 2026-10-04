package scheduler_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func recurringEmail(key string, seriesID uuid.UUID, at time.Time) task.Job {
	j := scheduledEmail(key, "email-high", 1, at)
	j.IsRecurring = true
	j.CronExpr = sql.NullString{String: "* * * * *", Valid: true}
	j.CronTZ = sql.NullString{String: "UTC", Valid: true}
	j.SeriesID = uuid.NullUUID{UUID: seriesID, Valid: true}
	return j
}

func TestTickRecurrence(t *testing.T) {
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Hour)

	t.Run("tick dispatches the occurrence and queues the next", func(t *testing.T) {
		sched, store, rds := setupScheduler(t)
		series := uuid.New()

		parent, err := store.CreateJob(ctx, recurringEmail("s-cron-1", series, past))
		require.NoError(t, err)

		n, err := sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		got, err := store.GetJobByID(ctx, parent.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got.Status)
		require.Equal(t, 1, streamLen(t, rds, queue.EmailHighStream))

		members, err := store.ListSeriesJobs(ctx, series, "", 50, 0)
		require.NoError(t, err)
		require.Len(t, members, 2)

		var child task.Job
		for _, m := range members {
			if m.JobID != parent.JobID {
				child = m
			}
		}
		require.Equal(t, task.JobScheduled, child.Status)
		require.True(t, child.ScheduledAt.Time.After(time.Now().UTC().Add(-time.Minute)))

		// Immediate re-tick dispatches nothing: the child is future.
		n, err = sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		require.Equal(t, 1, streamLen(t, rds, queue.EmailHighStream))
	})
}
