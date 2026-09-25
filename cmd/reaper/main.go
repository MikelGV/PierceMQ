package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/reaper"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "reaper exited: %v\n", err)
		os.Exit(1)
	}
}

// run connects Redis + Postgres (via PgBouncer, like the API and workers) and
// reclaims orphaned jobs until ctx is done: crashed-worker running jobs past
// the heartbeat timeout go back to pending and re-enter their stream, and
// stale pending jobs whose XADD never landed are re-dispatched. Stateless:
// all state lives in PostgreSQL, so a restart resumes where the last tick
// left off and multiple instances may run concurrently (SKIP LOCKED).
// It must run as its own process, never inside a worker pool — a crashed
// pool cannot run its own reaper (§12.3).
func run(ctx context.Context) error {
	rds, err := broker.Redis_Connect(config.Env.RedisURI)
	if err != nil {
		return fmt.Errorf("reaper: redis connect: %w", err)
	}
	defer rds.Conn.Close()

	stores, err := storage.Connect(ctx, config.Env.DB_URL, config.Env.DB_READ_URL)
	if err != nil {
		return fmt.Errorf("reaper: db connect: %w", err)
	}
	defer stores.Close()

	r := reaper.New(
		jobs.New(stores.Write.Conn, stores.Read.Conn),
		rds,
		time.Duration(config.Env.ReaperPollSeconds)*time.Second,
		int(config.Env.ReaperBatchSize),
		time.Duration(config.Env.ReaperStaleSeconds)*time.Second,
		time.Duration(config.Env.ReaperSweepAfterSeconds)*time.Second,
	).WithLog(os.Stdout)

	fmt.Fprintf(os.Stdout, "reaper: polling every %ds, stale-after %ds, sweep-after %ds, batch %d\n",
		config.Env.ReaperPollSeconds, config.Env.ReaperStaleSeconds,
		config.Env.ReaperSweepAfterSeconds, config.Env.ReaperBatchSize)
	return r.Run(ctx)
}
