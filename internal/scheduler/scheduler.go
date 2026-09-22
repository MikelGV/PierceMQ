// Package scheduler promotes due scheduled jobs to the Redis streams.
//
// Every PollInterval (default 10s per ARCHITECTURE.md §7.5) it claims a batch
// of jobs with scheduled_at <= now() via JobsStore.ClaimDueScheduled —
// SELECT ... FOR UPDATE SKIP LOCKED, so any number of scheduler instances may
// run concurrently without double-promotion — and XADDs each claimed job to
// its queue stream. Jobs whose XADD fails stay pending; the stale-pending
// sweeper (Phase 2) re-dispatches them. Stateless: all state lives in
// PostgreSQL, so a restart simply resumes polling.
package scheduler

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
	// DefaultPollInterval matches the 10s scheduler cadence in §7.5.
	DefaultPollInterval = 10 * time.Second
	// DefaultBatchSize caps jobs promoted per tick.
	DefaultBatchSize = 100
)

// StreamPusher dispatches a claimed job to its stream.
// *broker.RedisStore satisfies this via AddJobRefToStream.
type StreamPusher interface {
	AddJobRefToStream(ctx context.Context, stream string, job task.Job) (string, error)
}

// Scheduler polls PostgreSQL for due scheduled jobs and pushes them to the
// streams workers consume.
type Scheduler struct {
	jobs         *jobs.JobsStore
	redis        StreamPusher
	pollInterval time.Duration
	batchSize    int
	now          func() time.Time
	log          io.Writer
}

// New builds a Scheduler. Non-positive pollInterval/batchSize get defaults.
func New(j *jobs.JobsStore, r StreamPusher, pollInterval time.Duration, batchSize int) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &Scheduler{
		jobs:         j,
		redis:        r,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		now:          time.Now,
	}
}

// WithLog sets the writer for tick warnings; nil discards them.
func (s *Scheduler) WithLog(w io.Writer) *Scheduler {
	s.log = w
	return s
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.log == nil {
		return
	}
	fmt.Fprintf(s.log, format, args...)
}

// Tick claims one batch of due jobs and pushes each to its queue stream.
// It returns the number successfully dispatched. Claimed jobs whose stream is
// unknown or whose XADD fails are left pending (never rolled back): the
// stale-pending sweeper re-dispatches them. Tick itself only fails when the
// DB claim fails.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	due, err := s.jobs.ClaimDueScheduled(ctx, s.now().UTC(), s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("scheduler: claim due: %w", err)
	}
	dispatched := 0
	for _, job := range due {
		stream, _, err := queue.StreamFor(job.QueueName, job.Priority)
		if err != nil {
			s.logf("scheduler: job %s has unknown queue %q, left pending: %v\n", job.JobID, job.QueueName, err)
			continue
		}
		if s.redis == nil {
			s.logf("scheduler: job %s redis unavailable, left pending\n", job.JobID)
			continue
		}
		if _, err := s.redis.AddJobRefToStream(ctx, stream, job); err != nil {
			s.logf("scheduler: job %s XADD to %s failed, left pending: %v\n", job.JobID, stream, err)
			continue
		}
		dispatched++
	}
	return dispatched, nil
}

// Run ticks every poll interval until ctx is done. Tick errors are logged,
// not fatal: the next tick retries from PostgreSQL state.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := s.Tick(ctx); err != nil {
				s.logf("scheduler: tick failed: %v\n", err)
			}
		}
	}
}
