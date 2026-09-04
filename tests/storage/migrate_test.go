package storage_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/migrations"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// latestVersion counts distinct NNNNNN_ versions in the embedded FS so the
// assertions below track new migration files without manual bumps.
func latestVersion(t *testing.T) uint {
	t.Helper()
	entries, err := migrations.FS.ReadDir(".")
	require.NoError(t, err)
	var max uint
	for _, e := range entries {
		head, _, _ := strings.Cut(e.Name(), "_")
		v, err := strconv.Atoi(head)
		if err == nil && uint(v) > max {
			max = uint(v)
		}
	}
	require.Greater(t, max, uint(0))
	return max
}

// Requires a Docker daemon (same as the broker/worker integration tests).
func setupPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pgC, err := testcontainers.Run(ctx, "postgres:17-alpine",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     "admin",
			"POSTGRES_PASSWORD": "admin",
			"POSTGRES_DB":       "piercemq",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
			wait.ForLog("database system is ready to accept connections"),
		),
	)
	testcontainers.CleanupContainer(t, pgC)
	require.NoError(t, err)

	host, err := pgC.Host(ctx)
	require.NoError(t, err)
	port, err := pgC.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	return fmt.Sprintf("postgres://admin:admin@%s:%s/piercemq?sslmode=disable", host, port.Port())
}

func TestMigrateUpDown(t *testing.T) {
	ctx := context.Background()
	dsn := setupPostgres(t)
	latest := latestVersion(t)

	t.Run("up applies all migrations and records latest version", func(t *testing.T) {
		require.NoError(t, storage.MigrateUp(ctx, dsn))

		v, dirty, err := storage.MigrateVersion(ctx, dsn)
		require.NoError(t, err)
		require.Equal(t, latest, v)
		require.False(t, dirty)
	})

	t.Run("up is idempotent", func(t *testing.T) {
		require.NoError(t, storage.MigrateUp(ctx, dsn))

		v, dirty, err := storage.MigrateVersion(ctx, dsn)
		require.NoError(t, err)
		require.Equal(t, latest, v)
		require.False(t, dirty)
	})

	t.Run("down rolls back one step then up restores", func(t *testing.T) {
		require.NoError(t, storage.MigrateDown(ctx, dsn))

		v, dirty, err := storage.MigrateVersion(ctx, dsn)
		require.NoError(t, err)
		require.Equal(t, latest-1, v)
		require.False(t, dirty)

		require.NoError(t, storage.MigrateUp(ctx, dsn))
		v, dirty, err = storage.MigrateVersion(ctx, dsn)
		require.NoError(t, err)
		require.Equal(t, latest, v)
		require.False(t, dirty)
	})
}
