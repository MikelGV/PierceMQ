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

// backdateHeartbeat simulates a crashed worker: the row stays running but its
// heartbeat stops advancing.
func backdateHeartbeat(t *testing.T, db *sql.DB, jobID string, at time.Time) {
	t.Helper()
	_, err := db.Exec(`UPDATE jobs SET heartbeat_at = $1 WHERE job_id = $2`, at, jobID)
	require.NoError(t, err)
}

func TestClaimStaleRunning(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	staleBefore := now.Add(-90 * time.Second)

	t.Run("reclaims stale running to pending, clears claim, records event", func(t *testing.T) {
		store, db := setupJobsStore(t)

		created, err := store.CreateJob(ctx, baseJob("k-reap-1", sept2026()))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-dead")
		require.NoError(t, err)
		backdateHeartbeat(t, db, created.JobID.String(), now.Add(-5*time.Minute))

		got, err := store.ClaimStaleRunning(ctx, staleBefore, 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, created.JobID, got[0].JobID)
		require.Equal(t, task.JobPending, got[0].Status)
		require.False(t, got[0].WorkerID.Valid, "worker assignment cleared")
		require.False(t, got[0].ClaimToken.Valid, "claim token cleared")

		row, err := store.GetJobByID(ctx, created.JobID)
		require.NoError(t, err)
		require.Equal(t, task.JobPending, row.Status)
		require.Equal(t, 3, countEvents(t, db, created.JobID.String()))

		// Reclaimed jobs are claimable again by a healthy worker.
		_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-healthy")
		require.NoError(t, err)

		// Second sweep finds nothing.
		again, err := store.ClaimStaleRunning(ctx, staleBefore, 10)
		require.NoError(t, err)
		require.Empty(t, again)
	})

	t.Run("fresh heartbeats and terminal jobs untouched", func(t *testing.T) {
		store, db := setupJobsStore(t)

		fresh, err := store.CreateJob(ctx, baseJob("k-reap-2", sept2026()))
		require.NoError(t, err)
		_, _, err = store.ClaimRunning(ctx, fresh.JobID, "worker-alive")
		require.NoError(t, err)

		done, err := store.CreateJob(ctx, baseJob("k-reap-3", sept2026()))
		require.NoError(t, err)
		_, token, err := store.ClaimRunning(ctx, done.JobID, "worker-done")
		require.NoError(t, err)
		require.NoError(t, store.Complete(ctx, done.JobID, token))
		// Completed long ago: must never be resurrected.
		_, err = db.Exec(`UPDATE jobs SET heartbeat_at = $1 WHERE job_id = $2`,
			now.Add(-time.Hour), done.JobID.String())
		require.NoError(t, err)

		got, err := store.ClaimStaleRunning(ctx, staleBefore, 10)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("concurrent reapers reclaim each job exactly once", func(t *testing.T) {
		store, db := setupJobsStore(t)

		const n = 4
		for i := 0; i < n; i++ {
			created, err := store.CreateJob(ctx, baseJob(
				"k-reap-conc-"+string(rune('a'+i)), sept2026()))
			require.NoError(t, err)
			_, _, err = store.ClaimRunning(ctx, created.JobID, "worker-dead")
			require.NoError(t, err)
			backdateHeartbeat(t, db, created.JobID.String(), now.Add(-5*time.Minute))
		}

		var mu sync.Mutex
		seen := map[string]int{}
		var wg sync.WaitGroup
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := store.ClaimStaleRunning(ctx, staleBefore, 10)
				if err != nil {
					t.Errorf("ClaimStaleRunning: %v", err)
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

		require.Len(t, seen, n)
		for id, count := range seen {
			require.Equal(t, 1, count, "job %s reclaimed %d times", id, count)
		}
	})

	t.Run("empty when nothing stale", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		got, err := store.ClaimStaleRunning(ctx, staleBefore, 10)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("stale pending jobs are visible to the sweeper", func(t *testing.T) {
		store, _ := setupJobsStore(t)

		old := baseJob("k-sweep-1", sept2026())
		_, err := store.CreateJob(ctx, old)
		require.NoError(t, err)

		stale, err := store.ListStalePending(ctx, sept2026().Add(24*time.Hour), 50)
		require.NoError(t, err)
		require.Len(t, stale, 1)
		require.Equal(t, "k-sweep-1", stale[0].IdempotencyKey.String)
	})
}
