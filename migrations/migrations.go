package migrations

import "embed"

// FS embeds the *.sql migration files so the api binary carries its own
// schema. Run via `api migrate up` (see internal/storage/migrate.go),
// executed by the db-migrate one-shot in docker-compose.yaml.
//
// Naming: NNNNNN_name.up.sql / NNNNNN_name.down.sql, sequential versions.
// Multi-statement files are allowed (the runner enables x-multi-statement),
// but prefer one statement per file outside of partition bootstraps.
//
// PgBouncer runs in transaction mode: plain DDL is safe, but never use
// CONCURRENTLY operations or session-level statements in migrations.
//
//go:embed *.sql
var FS embed.FS
