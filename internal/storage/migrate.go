package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/MikelGV/PierceMQ/migrations"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// openMigrate builds a migrate instance backed by the embedded SQL files.
// Runs single-threaded with (at most) 2 connections: migrations execute
// from the db-migrate one-shot only, never from api replicas, so there is
// no concurrent-migrate race to guard against.
//
// dsn must target the WRITE pool (PgBouncer `piercemq` -> primary) and
// contain only real Postgres connection parameters. In particular, never
// append migrate driver options (e.g. x-multi-statement) to it: pgx
// forwards unknown query params as startup parameters and PgBouncer (like
// Postgres itself) rejects them with FATAL 08P01. Driver options belong
// on postgres.Config below.
// Plain DDL is safe in PgBouncer transaction mode; do not use
// CONCURRENTLY operations or session-level statements.
func openMigrate(ctx context.Context, dsn string) (*migrate.Migrate, *sql.DB, error) {
	if dsn == "" {
		return nil, nil, fmt.Errorf("storage: empty DSN")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: migrate open %s: %w", redact(dsn), err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("storage: migrate ping %s: %w", redact(dsn), err)
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("storage: migrate source: %w", err)
	}

	// MultiStatementEnabled splits multi-statement files (e.g. partition
	// bootstraps) on ';' in-process. Single-statement files behave
	// identically with it on.
	dbi, err := postgres.WithInstance(db, &postgres.Config{MultiStatementEnabled: true})
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("storage: migrate driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", dbi)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("storage: migrate instance: %w", err)
	}

	return m, db, nil
}

// MigrateUp applies all pending migrations. ErrNoChange is success.
func MigrateUp(ctx context.Context, dsn string) error {
	m, db, err := openMigrate(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// Close the migrate instance to release the source/db locks.
	defer func() {
		_, _ = m.Close()
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("storage: migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back the most recently applied migration.
func MigrateDown(ctx context.Context, dsn string) error {
	m, db, err := openMigrate(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	defer func() {
		_, _ = m.Close()
	}()

	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("storage: migrate down: %w", err)
	}
	return nil
}

// MigrateVersion reports the current schema version and dirty flag.
func MigrateVersion(ctx context.Context, dsn string) (uint, bool, error) {
	m, db, err := openMigrate(ctx, dsn)
	if err != nil {
		return 0, false, err
	}
	defer db.Close()
	defer func() {
		_, _ = m.Close()
	}()

	v, dirty, err := m.Version()
	if err != nil {
		return 0, false, fmt.Errorf("storage: migrate version: %w", err)
	}
	return v, dirty, nil
}

// MigrateForce clears a dirty migration state, forcing the version to v
// without running any SQL. Use only to repair a failed migration after
// inspecting schema_migrations manually.
func MigrateForce(ctx context.Context, dsn string, v int) error {
	m, db, err := openMigrate(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	defer func() {
		_, _ = m.Close()
	}()

	if err := m.Force(v); err != nil {
		return fmt.Errorf("storage: migrate force: %w", err)
	}
	return nil
}
