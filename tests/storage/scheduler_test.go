package storage_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/stretchr/testify/require"
)

// scheduledJob builds a scheduled job: created inside the Sep 2026 partition,
// firing at scheduledAt.
func scheduledJob(key string, scheduledAt time.Time) task.Job {
	j := baseJob(key, sept2026())
	j.Status = task.JobScheduled
	j.ScheduledAt = sql.NullTime{Time: scheduledAt, Valid: true}
	return j
}

func dueAt() time.Time {
	return sept2026().Add(-time.Hour)
}

func TestClaimDueScheduled(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("promotes due jobs to pending with events, leaves future alone", func(t *testing.T) {
		store, db := setupJobsStore(t)

		due1, err := store.CreateJob(ctx, scheduledJob("k-sched-1", dueAt()))
		require.NoError(t, err)
		due2, err := store.CreateJob(ctx, scheduledJob("k-sched-2", dueAt()))
		require.NoError(t, err)
		future, err := store.CreateJob(ctx, scheduledJob("k-sched-3", now.Add(24*time.Hour)))
		require.NoError(t, err)

		promoted, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Len(t, promoted, 2)
		for _, j := range promoted {
			require.Equal(t, task.JobPending, j.Status)
		}

		got1, err := store.GetJobByID(ctx, due1.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got1.Status)
		require.Equal(t, 2, countEvents(t, db, due1.JobID.String()))

		got2, err := store.GetJobByID(ctx, due2.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, got2.Status)

		still, err := store.GetJobByID(ctx, future.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobScheduled, still.Status)
		require.Equal(t, 1, countEvents(t, db, future.JobID.String()))

		// Second call finds nothing due.
		again, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Empty(t, again)
	})

	t.Run("limit is respected across calls", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		for _, k := range []string{"k-lim-1", "k-lim-2", "k-lim-3"} {
			_, err := store.CreateJob(ctx, scheduledJob(k, dueAt()))
			require.NoError(t, err)
		}

		first, err := store.ClaimDueScheduled(ctx, now, 2)
		require.NoError(t, err)
		require.Len(t, first, 2)

		second, err := store.ClaimDueScheduled(ctx, now, 2)
		require.NoError(t, err)
		require.Len(t, second, 1)
	})

	t.Run("concurrent schedulers promote each job exactly once", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		const n = 6
		for i := 0; i < n; i++ {
			_, err := store.CreateJob(ctx, scheduledJob(
				"k-conc-"+string(rune('a'+i)), dueAt()))
			require.NoError(t, err)
		}

		var mu sync.Mutex
		seen := map[string]int{}
		var wg sync.WaitGroup
		for w := 0; w < 3; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := store.ClaimDueScheduled(ctx, now, 10)
				if err != nil {
					t.Errorf("ClaimDueScheduled: %v", err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, j := range got {
					seen[j.JobID.String()]++
				}
			}()
		}
		wg.Wait()

		require.Len(t, seen, n, "every due job promoted exactly once across schedulers")
		for id, count := range seen {
			require.Equal(t, 1, count, "job %s claimed %d times", id, count)
		}
	})

	t.Run("empty when nothing due", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		got, err := store.ClaimDueScheduled(ctx, now, 10)
		require.NoError(t, err)
		require.Empty(t, got)
	})
}
