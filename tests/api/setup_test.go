package api_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/api"
	"github.com/MikelGV/PierceMQ/internal/api/routes"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgres mirrors tests/storage setup: ephemeral PG + all migrations.
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

	dsn := fmt.Sprintf("postgres://admin:admin@%s:%s/piercemq?sslmode=disable", host, port.Port())
	require.NoError(t, storage.MigrateUp(ctx, dsn))
	return dsn
}

type apiFixture struct {
	server *httptest.Server
	jobs   *jobs.JobsStore
	redis  *broker.RedisStore
	db     *sql.DB
}

// setupAPI wires a full HTTP server: migrated PG + test Redis + routes with a
// fixed test JWT secret.
func setupAPI(t *testing.T) *apiFixture {
	t.Helper()

	dsn := setupPostgres(t)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rds := utils_test.SetUpRedis(t)

	srv := httptest.NewServer(api.NewServer(routes.Deps{
		Redis:  rds,
		Config: &config.Config{JWTSecret: "test-secret-do-not-use", JWTTTLHours: 1},
		Users:  users.New(db, db),
		Keys:   auth.New(db, db),
		Jobs:   jobs.New(db, db),
	}))
	t.Cleanup(srv.Close)

	return &apiFixture{server: srv, jobs: jobs.New(db, db), redis: rds, db: db}
}
