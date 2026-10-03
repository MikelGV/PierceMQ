package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// callAPI issues an authed raw request and returns status + decoded body.
func callAPI(t *testing.T, c *client.Client, method, path string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, c.BaseURL+path, nil)
	require.NoError(t, err)
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func enqueueEmail(t *testing.T, c *client.Client, queueName string) string {
	t.Helper()
	out, err := c.Enqueue(context.Background(), client.EnqueueRequest{
		Type:      "email",
		QueueName: queueName,
		MaxRetry:  1,
		Payload:   map[string]any{"from": "a@example.com", "to": "b@example.com"},
	})
	require.NoError(t, err)
	id, ok := out["job_id"].(string)
	require.True(t, ok && id != "", "enqueue must return job_id: %v", out)
	return id
}

// forceFail drives a job to failed through the store (claim + exhaust).
func forceFail(t *testing.T, fx *apiFixture, jobID string) {
	t.Helper()
	ctx := context.Background()
	parsed, err := uuid.Parse(jobID)
	require.NoError(t, err)
	_, token, err := fx.jobs.ClaimRunning(ctx, parsed, "test-worker")
	require.NoError(t, err)
	_, err = fx.jobs.FailOrRetry(ctx, parsed, token, "boom")
	require.NoError(t, err)
}

func TestRetryEndpoint(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Retry", "retry@example.com")

	t.Run("failed job retries to pending and re-dispatches", func(t *testing.T) {
		id := enqueueEmail(t, c, "email-high")
		forceFail(t, fx, id)
		before, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)

		code, body := callAPI(t, c, http.MethodPost, "/v1/jobs/"+id+"/retry")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Equal(t, "pending", body["status"])
		require.NotEmpty(t, body["msg_id"], "retry must XADD: %v", body)

		after, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.Equal(t, before+1, after)

		got, err := c.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "pending", got["status"])
	})

	t.Run("non-failed retry is 409, unknown is 404, anon is 401", func(t *testing.T) {
		id := enqueueEmail(t, c, "email-high")
		code, _ := callAPI(t, c, http.MethodPost, "/v1/jobs/"+id+"/retry")
		require.Equal(t, http.StatusConflict, code)

		code, _ = callAPI(t, c, http.MethodPost, "/v1/jobs/"+uuid.New().String()+"/retry")
		require.Equal(t, http.StatusNotFound, code)

		anon := &client.Client{BaseURL: fx.server.URL}
		code, _ = callAPI(t, anon, http.MethodPost, "/v1/jobs/"+id+"/retry")
		require.Equal(t, http.StatusUnauthorized, code)
	})
}

func TestCancelEndpoint(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Cancel", "cancel@example.com")

	t.Run("pending job cancels and stays cancelled", func(t *testing.T) {
		id := enqueueEmail(t, c, "email-high")

		code, body := callAPI(t, c, http.MethodDelete, "/v1/jobs/"+id)
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Equal(t, "cancelled", body["status"])

		got, err := c.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "cancelled", got["status"])

		// Second cancel is a conflict, not a silent no-op.
		code, _ = callAPI(t, c, http.MethodDelete, "/v1/jobs/"+id)
		require.Equal(t, http.StatusConflict, code)
	})

	t.Run("running job cannot cancel, unknown is 404", func(t *testing.T) {
		id := enqueueEmail(t, c, "email-high")
		parsed, err := uuid.Parse(id)
		require.NoError(t, err)
		_, _, err = fx.jobs.ClaimRunning(ctx, parsed, "test-worker")
		require.NoError(t, err)

		code, _ := callAPI(t, c, http.MethodDelete, "/v1/jobs/"+id)
		require.Equal(t, http.StatusConflict, code)

		code, _ = callAPI(t, c, http.MethodDelete, "/v1/jobs/"+uuid.New().String())
		require.Equal(t, http.StatusNotFound, code)
	})
}

func TestListEndpoint(t *testing.T) {
	fx := setupAPI(t)
	c := authedClient(t, fx, "List", "list@example.com")

	id1 := enqueueEmail(t, c, "email-high")
	enqueueEmail(t, c, "email-low")
	id3 := enqueueEmail(t, c, "email-high")
	forceFail(t, fx, id3)

	t.Run("lists all newest with filter and paging", func(t *testing.T) {
		code, body := callAPI(t, c, http.MethodGet, "/v1/jobs")
		require.Equal(t, http.StatusOK, code, "%v", body)
		all, ok := body["jobs"].([]any)
		require.True(t, ok && len(all) == 3, "%v", body)

		code, body = callAPI(t, c, http.MethodGet, "/v1/jobs?status=failed")
		require.Equal(t, http.StatusOK, code, "%v", body)
		failed, ok := body["jobs"].([]any)
		require.True(t, ok && len(failed) == 1, "%v", body)
		require.Equal(t, id3, failed[0].(map[string]any)["job_id"])

		code, body = callAPI(t, c, http.MethodGet, "/v1/jobs?status=pending")
		require.Equal(t, http.StatusOK, code, "%v", body)
		pending := body["jobs"].([]any)
		require.Len(t, pending, 2)

		code, body = callAPI(t, c, http.MethodGet, "/v1/jobs?limit=2&offset=0")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Len(t, body["jobs"].([]any), 2)

		code, body = callAPI(t, c, http.MethodGet, "/v1/jobs?limit=2&offset=2")
		require.Equal(t, http.StatusOK, code, "%v", body)
		page2 := body["jobs"].([]any)
		require.Len(t, page2, 1)
		require.Equal(t, id1, page2[0].(map[string]any)["job_id"], "oldest lands on page 2")
	})

	t.Run("bad filter and paging are 400", func(t *testing.T) {
		code, _ := callAPI(t, c, http.MethodGet, "/v1/jobs?status=bogus")
		require.Equal(t, http.StatusBadRequest, code)

		code, _ = callAPI(t, c, http.MethodGet, "/v1/jobs?limit=abc")
		require.Equal(t, http.StatusBadRequest, code)

		code, _ = callAPI(t, c, http.MethodGet, "/v1/jobs?offset=-1")
		require.Equal(t, http.StatusBadRequest, code)
	})
}

func TestStatsEndpoint(t *testing.T) {
	fx := setupAPI(t)
	c := authedClient(t, fx, "Stats", "stats@example.com")

	enqueueEmail(t, c, "email-high")
	enqueueEmail(t, c, "email-high")
	id3 := enqueueEmail(t, c, "email-low")
	forceFail(t, fx, id3)

	code, body := callAPI(t, c, http.MethodGet, "/v1/stats")
	require.Equal(t, http.StatusOK, code, "%v", body)
	for _, key := range []string{"pending", "queued", "processing", "completed", "failed", "scheduled", "cancelled", "pending_oldest_age_sec", "total_workers", "queue_depth"} {
		require.Contains(t, body, key, "stats must carry %s: %v", key, body)
	}
	require.Equal(t, float64(2), body["pending"])
	require.Equal(t, float64(1), body["failed"])
	require.GreaterOrEqual(t, body["queue_depth"].(float64), float64(3))
}

func TestClientOpsParity(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "SDK", "sdk@example.com")

	id1 := enqueueEmail(t, c, "email-high")
	id2 := enqueueEmail(t, c, "email-low")
	forceFail(t, fx, id2)

	listed, err := c.ListJobs(ctx, "", 0, 0)
	require.NoError(t, err)
	require.Len(t, listed["jobs"].([]any), 2)

	failedOnly, err := c.ListJobs(ctx, "failed", 0, 0)
	require.NoError(t, err)
	require.Len(t, failedOnly["jobs"].([]any), 1)

	retried, err := c.Retry(ctx, id2)
	require.NoError(t, err)
	require.Equal(t, "pending", retried["status"])

	cancelled, err := c.Cancel(ctx, id1)
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled["status"])

	stats, err := c.Stats(ctx)
	require.NoError(t, err)
	for _, key := range []string{"pending", "queued", "processing", "completed", "failed", "scheduled", "cancelled", "pending_oldest_age_sec", "total_workers", "queue_depth"} {
		require.Contains(t, stats, key)
	}
}

func TestHealthAlias(t *testing.T) {
	fx := setupAPI(t)
	c := authedClient(t, fx, "Health", "health@example.com")

	for _, path := range []string{"/healthz", "/health"} {
		code, body := callAPI(t, c, http.MethodGet, path)
		require.Equal(t, http.StatusOK, code, "%s: %v", path, body)
		require.Equal(t, "ok", body["status"])
	}
}
