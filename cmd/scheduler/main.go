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
	"github.com/MikelGV/PierceMQ/internal/scheduler"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "scheduler exited: %v\n", err)
		os.Exit(1)
	}
}

// run connects Redis + Postgres (via PgBouncer, like the API and workers) and
// polls for due scheduled jobs until ctx is done. Stateless: all scheduling
// state lives in PostgreSQL, so a restart resumes where the last tick left
// off and multiple instances may run concurrently (SKIP LOCKED claiming).
func run(ctx context.Context) error {
	rds, err := broker.ConnectFromConfig(config.Env.RedisURI, config.Env.RedisSentinels, config.Env.RedisMasterName)
	rds.MaxLen = config.Env.StreamMaxLen
	if err != nil {
		return fmt.Errorf("scheduler: redis connect: %w", err)
	}
	defer rds.Conn.Close()

	stores, err := storage.Connect(ctx, config.Env.DB_URL, config.Env.DB_READ_URL)
	if err != nil {
		return fmt.Errorf("scheduler: db connect: %w", err)
	}
	defer stores.Close()

	poll := time.Duration(config.Env.SchedPollSeconds) * time.Second
	s := scheduler.New(
		jobs.New(stores.Write.Conn, stores.Read.Conn),
		rds,
		poll,
		int(config.Env.SchedBatchSize),
	).WithLog(os.Stdout)

	fmt.Fprintf(os.Stdout, "scheduler: polling every %s, batch %d\n",
		poll, config.Env.SchedBatchSize)

	retentionPoll := time.Duration(config.Env.RetentionPollSeconds) * time.Second
	if retentionPoll <= 0 {
		retentionPoll = scheduler.DefaultRetentionPoll
	}
	retentionBatch := int(config.Env.RetentionBatchSize)
	if retentionBatch <= 0 {
		retentionBatch = scheduler.DefaultRetentionBatch
	}
	store := jobs.New(stores.Write.Conn, stores.Read.Conn)
	go func() {
		t := time.NewTicker(retentionPoll)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := scheduler.Maintain(ctx, store, time.Now().UTC(), config.Env.RetentionDays, retentionBatch)
				if err != nil {
					fmt.Fprintf(os.Stdout, "scheduler: retention cycle failed: %v\n", err)
					continue
				}
				if n > 0 {
					fmt.Fprintf(os.Stdout, "scheduler: retention purged %d jobs\n", n)
				}
			}
		}
	}()

	return s.Run(ctx)
}
