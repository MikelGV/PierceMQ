package migrations

import "embed"

// FS embeds the *.sql migration files so the api binary carries its own
// schema. Run via `api migrate up` (see internal/storage/migrate.go),
// executed by the db-migrate one-shot in docker-compose.yaml.
//
// Naming: NNNNNN_name.up.sql / NNNNNN_name.down.sql, sequential versions.
// Multi-statement files are allowed (the runner sets MultiStatementEnabled
// on the migrate driver; never put driver options in the DSN), but prefer
// one statement per file outside of partition bootstraps.
//
// NO `--` COMMENTS IN .sql FILES: with MultiStatementEnabled the migrate
// driver mangles leading `--` lines (strips the marker, keeps the text),
// producing syntax errors. Document migrations here, never in the SQL.
//
// PgBouncer runs in transaction mode: plain DDL is safe, but never use
// CONCURRENTLY operations or session-level statements in migrations.
//
// Schema notes:
//   - 000002 defines the job_status enum (incl. 'scheduled').
//   - 000003 creates partitioned jobs(parent only); PK (job_id, created_at).
//   - 000004 bootstraps jobs monthlies Sep-Nov 2026 + DEFAULT.
//   - 000005 creates partitioned job_events (parent only, no FK: a FK to
//     jobs(job_id) is illegal without a matching unique constraint).
//   - 000006 defines parent-level indexes (auto-propagate as local
//     indexes); the idempotency UNIQUE is on (idempotency_key, created_at)
//     because partitioned UNIQUEs must include the partition key --
//     cross-month duplicates still do not conflict. All CREATE INDEX use
//     IF NOT EXISTS so a dirty-repaired v6 re-runs cleanly.
//   - 000007 bootstraps job_events monthlies Sep-Nov 2026 + DEFAULT.
//   - Partition coverage ends Nov 2026: add next monthlies before Dec 2026.
//   - 000008 creates users (auth MVP: email unique, bcrypt password_hash).
//   - 000009 creates api_keys (SHA256 key_hash unique, per-user lookup
//     index, partial index on active hashes for Bearer auth).
//
//go:embed *.sql
var FS embed.FS
