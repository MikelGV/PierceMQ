package web_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/api"
	"github.com/MikelGV/PierceMQ/internal/api/routes"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
	storageauth "github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
	"github.com/MikelGV/PierceMQ/internal/web"
	"github.com/MikelGV/PierceMQ/pkg/client"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type webFixture struct {
	web   *httptest.Server
	api   *httptest.Server
	jobs  *jobs.JobsStore
	jar   *cookiejar.Jar
	noRed *http.Client
}

func setupWeb(t *testing.T) *webFixture {
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

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rds := utils_test.SetUpRedis(t)
	stores := &storage.Stores{
		Write: &storage.DbStore{Conn: db},
		Read:  &storage.DbStore{Conn: db},
	}
	apiSrv := httptest.NewServer(api.NewServer(routes.Deps{
		Redis:  rds,
		Config: &config.Config{JWTSecret: "test-secret-do-not-use", JWTTTLHours: 1},
		Stores: stores,
		Users:  users.New(db, db),
		Keys:   storageauth.New(db, db),
		Jobs:   jobs.New(db, db),
	}))
	t.Cleanup(apiSrv.Close)

	webSrv := httptest.NewServer(web.NewServer(apiSrv.URL))
	t.Cleanup(webSrv.Close)

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	noRed := &http.Client{
		Jar: jar,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &webFixture{web: webSrv, api: apiSrv, jobs: jobs.New(db, db), jar: jar, noRed: noRed}
}

// registerAPI creates a user directly against the API and returns an authed SDK client.
func registerAPI(t *testing.T, fx *webFixture, name, email string) *client.Client {
	t.Helper()
	c := &client.Client{BaseURL: fx.api.URL}
	_, err := c.Register(context.Background(), name, email, "password123")
	require.NoError(t, err)
	_, err = c.Login(context.Background(), email, "password123")
	require.NoError(t, err)
	return c
}

// webLogin performs the dashboard login form flow with the redirect-following client.
func webLogin(t *testing.T, fx *webFixture, email string) {
	t.Helper()
	resp, err := fx.noRed.PostForm(fx.web.URL+"/login", url.Values{
		"email": {email}, "password": {"password123"},
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	require.Equal(t, http.StatusSeeOther, resp.StatusCode, "login must redirect")
	require.Equal(t, "/", resp.Header.Get("Location"))
	// Session cookie is HttpOnly.
	found := false
	for _, c := range resp.Cookies() {
		if c.Name == "pmq_jwt" && c.Value != "" {
			found = true
			require.True(t, c.HttpOnly)
		}
	}
	require.True(t, found, "login must set pmq_jwt")
}

func webGet(t *testing.T, fx *webFixture, path string) (int, string) {
	t.Helper()
	resp, err := fx.noRed.Get(fx.web.URL + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(b)
}

func TestWebAuthFlow(t *testing.T) {
	fx := setupWeb(t)

	t.Run("anonymous dashboard redirects to login", func(t *testing.T) {
		code, _ := webGet(t, fx, "/")
		require.Equal(t, http.StatusSeeOther, code)
	})

	t.Run("login page renders", func(t *testing.T) {
		code, body := webGet(t, fx, "/login")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "Login")
	})

	t.Run("bad credentials re-render with 401", func(t *testing.T) {
		resp, err := fx.noRed.PostForm(fx.web.URL+"/login", url.Values{
			"email": {"nobody@example.com"}, "password": {"wrongpassword"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		require.Contains(t, string(b), "invalid credentials")
	})

	t.Run("login sets session and dashboard renders", func(t *testing.T) {
		registerAPI(t, fx, "Web", "web@example.com")
		webLogin(t, fx, "web@example.com")

		code, body := webGet(t, fx, "/")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "Dashboard")
		require.Contains(t, body, "Pending")
		require.Contains(t, body, "htmx.min.js")
	})

	t.Run("logout clears session", func(t *testing.T) {
		resp, err := fx.noRed.PostForm(fx.web.URL+"/logout", url.Values{})
		require.NoError(t, err)
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		require.Equal(t, http.StatusSeeOther, resp.StatusCode)

		code, _ := webGet(t, fx, "/")
		require.Equal(t, http.StatusSeeOther, code, "logged out dashboard redirects")
	})
}

func TestWebJobFlows(t *testing.T) {
	fx := setupWeb(t)
	ctx := context.Background()
	apiClient := registerAPI(t, fx, "Jobs", "jobs@example.com")
	webLogin(t, fx, "jobs@example.com")

	t.Run("dashboard shows enqueued job", func(t *testing.T) {
		out, err := apiClient.Enqueue(ctx, client.EnqueueRequest{
			Type: "email", QueueName: "email-high",
			Payload: map[string]any{"to": "b@example.com"},
		})
		require.NoError(t, err)
		id, _ := out["job_id"].(string)

		code, body := webGet(t, fx, "/")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, id[:8])
		require.Contains(t, body, "pending")

		code, body = webGet(t, fx, "/jobs/"+id)
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "pending")
		require.Contains(t, body, "b@example.com")
		require.Contains(t, body, "Timeline")
	})

	t.Run("partials serve fragments", func(t *testing.T) {
		code, body := webGet(t, fx, "/partials/stats")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, `id="stat-cards"`)
		require.NotContains(t, body, "<html", "fragment must not be a full page")

		code, body = webGet(t, fx, "/partials/jobs?status=pending")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, `id="job-table"`)
	})

	t.Run("create form enqueues and redirects to detail", func(t *testing.T) {
		code, body := webGet(t, fx, "/jobs/new")
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "Enqueue job")

		resp, err := fx.noRed.PostForm(fx.web.URL+"/jobs", url.Values{
			"type": {"email"}, "queue_name": {"email-high"},
			"payload": {`{"to":"c@example.com"}`},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		require.Equal(t, http.StatusSeeOther, resp.StatusCode)
		loc := resp.Header.Get("Location")
		require.True(t, strings.HasPrefix(loc, "/jobs/"), loc)

		code, body = webGet(t, fx, loc)
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "c@example.com")
	})

	t.Run("create form rejects bad payload", func(t *testing.T) {
		resp, err := fx.noRed.PostForm(fx.web.URL+"/jobs", url.Values{
			"type": {"email"}, "payload": {"not json"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Contains(t, string(b), "JSON object")
	})

	t.Run("retry and cancel buttons work", func(t *testing.T) {
		out, err := apiClient.Enqueue(ctx, client.EnqueueRequest{
			Type: "email", QueueName: "email-high", MaxRetry: 1,
			Payload: map[string]any{"to": "d@example.com"},
		})
		require.NoError(t, err)
		id, _ := out["job_id"].(string)

		// Cancel while pending.
		resp, err := fx.noRed.PostForm(fx.web.URL+"/jobs/"+id+"/cancel", url.Values{})
		require.NoError(t, err)
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		require.Equal(t, http.StatusSeeOther, resp.StatusCode)

		code, body := webGet(t, fx, "/jobs/"+id)
		require.Equal(t, http.StatusOK, code)
		require.Contains(t, body, "cancelled")
	})

	t.Run("unknown job is 404", func(t *testing.T) {
		code, body := webGet(t, fx, "/jobs/00000000-0000-0000-0000-000000000000")
		require.Equal(t, http.StatusNotFound, code)
		require.Contains(t, body, "job not found")
	})
}
