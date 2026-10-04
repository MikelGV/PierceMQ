package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/api"
	"github.com/MikelGV/PierceMQ/internal/api/routes"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage"
	storageauth "github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
	"github.com/MikelGV/PierceMQ/pkg/client"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
)

// setupAPICustom mirrors setupAPI with caller-controlled rate limits.
func setupAPICustom(t *testing.T, rps, burst int64) *apiFixture {
	t.Helper()

	dsn := setupPostgres(t)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rds := utils_test.SetUpRedis(t)

	stores := &storage.Stores{
		Write: &storage.DbStore{Conn: db},
		Read:  &storage.DbStore{Conn: db},
	}

	srv := httptest.NewServer(api.NewServer(routes.Deps{
		Redis: rds,
		Config: &config.Config{
			JWTSecret: "test-secret-do-not-use", JWTTTLHours: 1,
			RateLimitRPS: rps, RateLimitBurst: burst,
		},
		Stores: stores,
		Users:  users.New(db, db),
		Keys:   storageauth.New(db, db),
		Jobs:   jobs.New(db, db),
	}))
	t.Cleanup(srv.Close)

	return &apiFixture{server: srv, jobs: jobs.New(db, db), redis: rds, db: db}
}

// callAPIWithHeaders issues a raw request with extra headers.
func callAPIWithHeaders(t *testing.T, c *client.Client, method, path string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, c.BaseURL+path, nil)
	require.NoError(t, err)
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestRateLimiting(t *testing.T) {
	fx := setupAPICustom(t, 1, 3)
	ctx := context.Background()
	// Register+login ride the shared IP bucket; authed calls below ride a
	// fresh per-token bucket of 3.
	c := authedClient(t, fx, "Rate", "rate@example.com")

	for i := 0; i < 3; i++ {
		code, _ := callAPI(t, c, http.MethodGet, "/v1/stats")
		require.Equal(t, http.StatusOK, code, "request %d within burst", i+1)
	}

	t.Run("over burst is 429 with Retry-After", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/stats", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+c.Token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		require.NotEmpty(t, resp.Header.Get("Retry-After"))
	})

	t.Run("health probes are exempt", func(t *testing.T) {
		resp, err := http.Get(fx.server.URL + "/healthz")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("bucket refills", func(t *testing.T) {
		time.Sleep(1200 * time.Millisecond)
		code, _ := callAPI(t, c, http.MethodGet, "/v1/stats")
		require.Equal(t, http.StatusOK, code)
	})
}

func TestBodyCaps(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Cap", "cap@example.com")

	postRaw := func(body string, chunked bool) int {
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodPost, c.BaseURL+"/v1/jobs", strings.NewReader(body))
		if err != nil {
			t.Error(err)
			return 0
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		req.Header.Set("Content-Type", "application/json")
		if chunked {
			req.ContentLength = -1
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	t.Run("oversize envelope is 413", func(t *testing.T) {
		// Legal payload, but ~300KB of ignored keys bypass the 15KB
		// payload check — the envelope cap must catch it.
		junk := strings.Repeat("x", 300*1024)
		body := `{"type":"email","queue_name":"email-high","payload":{"a":"b"},"junk":"` + junk + `"}`
		require.Equal(t, http.StatusRequestEntityTooLarge, postRaw(body, false))
	})

	t.Run("oversize chunked envelope is 413", func(t *testing.T) {
		junk := strings.Repeat("y", 300*1024)
		body := `{"type":"email","queue_name":"email-high","payload":{"a":"b"},"junk":"` + junk + `"}`
		require.Equal(t, http.StatusRequestEntityTooLarge, postRaw(body, true))
	})

	t.Run("oversize auth envelope is 413", func(t *testing.T) {
		big := strings.Repeat("n", 100*1024)
		raw, _ := json.Marshal(map[string]string{
			"name": big, "email": "big@example.com", "password": "password123",
		})
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodPost, c.BaseURL+"/v1/auth/register", strings.NewReader(string(raw)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	})

	t.Run("normal enqueue still 201", func(t *testing.T) {
		out, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type: "email", QueueName: "email-high",
			Payload: map[string]any{"to": "b@example.com"},
		})
		require.NoError(t, err)
		require.Equal(t, "pending", out["status"])
	})
}

func TestRetryIdempotencyKey(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "RK", "rk@example.com")

	id := enqueueEmail(t, c, "email-high")
	forceFail(t, fx, id)

	t.Run("first keyed retry dispatches", func(t *testing.T) {
		before, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)

		out, err := c.RetryWithKey(ctx, id, "retry-key-1")
		require.NoError(t, err)
		require.Equal(t, "pending", out["status"])
		require.NotEmpty(t, out["msg_id"])

		after, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.Equal(t, before+1, after)
	})

	t.Run("replay returns current state without redispatch", func(t *testing.T) {
		before, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)

		out, err := c.RetryWithKey(ctx, id, "retry-key-1")
		require.NoError(t, err)
		require.Equal(t, "pending", out["status"])
		require.Equal(t, true, out["deduplicated"])
		_, hasMsg := out["msg_id"]
		require.False(t, hasMsg, "replay must not XADD: %v", out)

		after, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.Equal(t, before, after)
	})

	t.Run("key bound to another job is 409", func(t *testing.T) {
		id2 := enqueueEmail(t, c, "email-high")
		forceFail(t, fx, id2)

		code, _ := callAPIWithHeaders(t, c, http.MethodPost, "/v1/jobs/"+id2+"/retry",
			map[string]string{"Idempotency-Key": "retry-key-1"})
		require.Equal(t, http.StatusConflict, code)
	})

	t.Run("fresh key on second job works", func(t *testing.T) {
		id3 := enqueueEmail(t, c, "email-high")
		forceFail(t, fx, id3)
		out, err := c.RetryWithKey(ctx, id3, "retry-key-2")
		require.NoError(t, err)
		require.Equal(t, "pending", out["status"])
	})
}

func TestBarePathAliases(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Bare", "bare@example.com")

	postBare := func(path, body string, token string) (int, map[string]any) {
		var rdr *strings.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		} else {
			rdr = strings.NewReader("")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, rdr)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	t.Run("bare submit/get/list/stats round-trip", func(t *testing.T) {
		code, out := postBare("/jobs",
			`{"type":"email","queue_name":"email-high","payload":{"to":"b"}}`, c.Token)
		require.Equal(t, http.StatusCreated, code, "%v", out)
		id, _ := out["job_id"].(string)
		require.NotEmpty(t, id)

		code, body := callAPI(t, c, http.MethodGet, "/jobs/"+id)
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Equal(t, "pending", body["status"])

		code, body = callAPI(t, c, http.MethodGet, "/jobs?status=pending")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.NotEmpty(t, body["jobs"])

		code, body = callAPI(t, c, http.MethodGet, "/stats")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Contains(t, body, "pending")

		code, _ = callAPI(t, c, http.MethodDelete, "/jobs/"+id)
		require.Equal(t, http.StatusOK, code)
	})

	t.Run("bare retry replays failed job", func(t *testing.T) {
		_, out := postBare("/jobs",
			`{"type":"email","queue_name":"email-high","max_retry":1,"payload":{"to":"b"}}`, c.Token)
		id, _ := out["job_id"].(string)
		forceFail(t, fx, id)

		code, body := postBare("/jobs/"+id+"/retry", "", c.Token)
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Equal(t, "pending", body["status"])
	})

	t.Run("bare paths require auth", func(t *testing.T) {
		for _, path := range []string{"/jobs", "/stats"} {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		}
	})

	t.Run("v1 still canonical", func(t *testing.T) {
		code, out := postBare("/jobs",
			`{"type":"email","queue_name":"email-high","payload":{"to":"b"}}`, c.Token)
		require.Equal(t, http.StatusCreated, code)
		id, _ := out["job_id"].(string)
		require.NotEmpty(t, id)

		got, err := c.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "pending", got["status"])
	})
}
