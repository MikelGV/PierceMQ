package jobs

import (
	"context"
	"fmt"
	"time"
)

// DeleteOldJobs removes terminal jobs past retention: completed/cancelled
// rows with completed_at older than before (ARCHITECTURE.md §10.4: 4-day
// retention). Failed jobs are excluded: they sit in the DLQ until resolved
// or explicitly purged. Bounded by limit per call so a large backlog drains
// over successive ticks without a long write lock. Dangling idempotency
// keys for the deleted jobs are removed in the same call. Write pool.
func (s *JobsStore) DeleteOldJobs(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		limit = 10000
	}

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`WITH doomed AS (
			SELECT job_id, created_at FROM jobs
			WHERE status IN ('completed', 'cancelled') AND completed_at < $1
			ORDER BY completed_at LIMIT $2
		)
		DELETE FROM jobs j USING doomed d
		WHERE j.job_id = d.job_id AND j.created_at = d.created_at`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old jobs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete old jobs rows: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM job_idempotency k USING (
			SELECT k2.idempotency_key FROM job_idempotency k2
			LEFT JOIN jobs j ON j.job_id = k2.job_id
			WHERE j.job_id IS NULL
			LIMIT $1
		) orphan
		WHERE k.idempotency_key = orphan.idempotency_key`, limit); err != nil {
		return 0, fmt.Errorf("delete orphan idempotency: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}

// EnsureMonthlyPartitions pre-creates jobs/job_events monthly partitions for
// the current month plus ahead further months (idempotent). Beyond the
// migration-bootstrapped window this is what keeps inserts from landing in
// the DEFAULT partition. Write pool; safe under concurrency (IF NOT EXISTS).
func (s *JobsStore) EnsureMonthlyPartitions(ctx context.Context, now time.Time, ahead int) error {
	if ahead < 0 {
		ahead = 0
	}
	if ahead > 12 {
		ahead = 12
	}
	base := time.Date(now.UTC().Year(), now.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i <= ahead; i++ {
		start := base.AddDate(0, i, 0)
		end := start.AddDate(0, 1, 0)
		name := fmt.Sprintf("jobs_p%04d_%02d", start.Year(), int(start.Month()))
		if _, err := s.write.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF jobs FOR VALUES FROM ('%s') TO ('%s')`,
			name, start.Format("2006-01-02"), end.Format("2006-01-02"))); err != nil {
			return fmt.Errorf("ensure jobs partition %s: %w", name, err)
		}
		ename := fmt.Sprintf("job_events_p%04d_%02d", start.Year(), int(start.Month()))
		if _, err := s.write.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF job_events FOR VALUES FROM ('%s') TO ('%s')`,
			ename, start.Format("2006-01-02"), end.Format("2006-01-02"))); err != nil {
			return fmt.Errorf("ensure job_events partition %s: %w", ename, err)
		}
	}
	return nil
}
