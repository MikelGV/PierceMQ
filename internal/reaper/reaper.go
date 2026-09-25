// Package reaper reclaims orphaned jobs and re-enqueues them.
//
// Two recovery paths, one tick (ARCHITECTURE.md §12.3):
//
//  1. Crashed workers — jobs stuck in running whose heartbeat_at is older
//     than StaleAfter (default 90s = 3x the heartbeat interval, §12.2) are
//     reset to pending (claim cleared) and XADDed back to their stream for a
//     healthy worker to claim.
//  2. Stale pending sweep — jobs stuck in pending/queued older than
//     SweepAfter (default 5m), i.e. orphans of a DB-insert/XADD dual write
//     where the XADD failed, are re-XADDed. The cutoff keeps healthy-path
//     churn at zero (live jobs are claimed in seconds); duplicates under a
//     sustained backlog are absorbed by the ClaimRunning status guard
//     (redeliveries ack + skip) and bounded by stream MAXLEN (Phase 4).
//
// The reaper runs as its own process (cmd/reaper), never inside a worker
// pool — a crashed pool cannot run its own reaper. It is stateless and
// horizontally safe (SKIP LOCKED claiming); like the scheduler, any number of
// instances may run concurrently.
package reaper

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
)

const (
	// DefaultPollInterval is how often the reaper ticks.
	DefaultPollInterval = 30 * time.Second
	// DefaultBatchSize caps jobs reclaimed/swept per tick.
	DefaultBatchSize = 100
	// DefaultStaleAfter is the heartbeat timeout from §12.2.
	DefaultStaleAfter = 90 * time.Second
	// DefaultSweepAfter is how old a pending job must be before the sweeper
	// re-dispatches it.
	DefaultSweepAfter = 5 * time.Minute
)

// StreamPusher dispatches a job to its stream.
// *broker.RedisStore satisfies this via AddJobRefToStream.
type StreamPusher interface {
	AddJobRefToStream(ctx context.Context, stream string, job task.Job) (string, error)
}

// TickStats reports what one Tick did.
type TickStats struct {
	// Reclaimed is stale running rows moved back to pending.
	Reclaimed int
	// Reenqueued is reclaimed rows successfully XADDed.
	Reenqueued int
	// Swept is stale pending rows re-XADDed.
	Swept int
}

// Reaper reclaims crashed-worker jobs and sweeps stale pending jobs.
type Reaper struct {
	jobs         *jobs.JobsStore
	redis        StreamPusher
	pollInterval time.Duration
	batchSize    int
	staleAfter   time.Duration
	sweepAfter   time.Duration
	now          func() time.Time
	log          io.Writer
}

// New builds a Reaper. Non-positive intervals/sizes get defaults.
func New(j *jobs.JobsStore, r StreamPusher, pollInterval time.Duration, batchSize int, staleAfter, sweepAfter time.Duration) *Reaper {
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if staleAfter <= 0 {
		staleAfter = DefaultStaleAfter
	}
	if sweepAfter <= 0 {
		sweepAfter = DefaultSweepAfter
	}
	return &Reaper{
		jobs:         j,
		redis:        r,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		staleAfter:   staleAfter,
		sweepAfter:   sweepAfter,
		now:          time.Now,
	}
}

// WithLog sets the writer for tick warnings; nil discards them.
func (r *Reaper) WithLog(w io.Writer) *Reaper {
	r.log = w
	return r
}

func (r *Reaper) logf(format string, args ...any) {
	if r.log == nil {
		return
	}
	fmt.Fprintf(r.log, format, args...)
}

// dispatch XADDs one job to its queue stream. Callers leave the (pending) row
// alone on failure — the next tick's sweep retries it.
func (r *Reaper) dispatch(ctx context.Context, job task.Job, why string) bool {
	stream, _, err := queue.StreamFor(job.QueueName, job.Priority)
	if err != nil {
		r.logf("reaper: job %s has unknown queue %q (%s), left pending: %v\n", job.JobID, job.QueueName, why, err)
		return false
	}
	if r.redis == nil {
		r.logf("reaper: job %s redis unavailable (%s), left pending\n", job.JobID, why)
		return false
	}
	if _, err := r.redis.AddJobRefToStream(ctx, stream, job); err != nil {
		r.logf("reaper: job %s XADD to %s failed (%s), left pending: %v\n", job.JobID, stream, why, err)
		return false
	}
	return true
}

// Tick reclaims one batch of stale running jobs and sweeps one batch of stale
// pending jobs. It only fails when a DB call fails; dispatch failures leave
// rows pending for the next tick.
func (r *Reaper) Tick(ctx context.Context) (TickStats, error) {
	var stats TickStats
	now := r.now().UTC()

	stale, err := r.jobs.ClaimStaleRunning(ctx, now.Add(-r.staleAfter), r.batchSize)
	if err != nil {
		return stats, fmt.Errorf("reaper: claim stale: %w", err)
	}
	stats.Reclaimed = len(stale)
	// Jobs reclaimed above were just dispatched (or left pending on dispatch
	// failure); either way the sweep below must skip them this tick to avoid
	// an immediate self-duplicate. Across ticks, still-pending old jobs are
	// swept again by design — duplicates are absorbed by the ClaimRunning
	// status guard and bounded by stream MAXLEN (Phase 4).
	reclaimed := make(map[string]struct{}, len(stale))
	for _, job := range stale {
		reclaimed[job.JobID.String()] = struct{}{}
		if r.dispatch(ctx, job, "reclaim") {
			stats.Reenqueued++
		}
	}

	pending, err := r.jobs.ListStalePending(ctx, now.Add(-r.sweepAfter), r.batchSize)
	if err != nil {
		return stats, fmt.Errorf("reaper: list stale pending: %w", err)
	}
	for _, job := range pending {
		if _, ok := reclaimed[job.JobID.String()]; ok {
			continue
		}
		if r.dispatch(ctx, job, "sweep") {
			stats.Swept++
		}
	}
	return stats, nil
}

// Run ticks every poll interval until ctx is done. Tick errors are logged,
// not fatal: the next tick retries from PostgreSQL state.
func (r *Reaper) Run(ctx context.Context) error {
	t := time.NewTicker(r.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := r.Tick(ctx); err != nil {
				r.logf("reaper: tick failed: %v\n", err)
			}
		}
	}
}
