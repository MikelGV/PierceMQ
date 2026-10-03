package scheduler

import (
	"context"
	"time"

	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
)

// DefaultRetentionPoll is the fallback purger cadence when unconfigured.
const DefaultRetentionPoll = time.Hour

// DefaultRetentionBatch caps rows purged per cycle when unconfigured.
const DefaultRetentionBatch = 1000

// PartitionsAhead is how many future monthly partitions each cycle ensures.
const PartitionsAhead = 2

// Maintain runs one retention cycle: purge terminal jobs older than
// retentionDays and ensure current + future monthly partitions exist.
// Non-positive retentionDays disables purging (partitions are still
// ensured). Returns rows purged.
func Maintain(ctx context.Context, store *jobs.JobsStore, now time.Time, retentionDays int64, batch int) (int64, error) {
	if err := store.EnsureMonthlyPartitions(ctx, now, PartitionsAhead); err != nil {
		return 0, err
	}
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoff := now.UTC().AddDate(0, 0, -int(retentionDays))
	return store.DeleteOldJobs(ctx, cutoff, batch)
}
