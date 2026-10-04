package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/scheduler"
	"github.com/MikelGV/PierceMQ/pkg/client"
	"github.com/stretchr/testify/require"
)

func enqueueCron(t *testing.T, c *client.Client, cron, tz string) map[string]any {
	t.Helper()
	out, err := c.Enqueue(context.Background(), client.EnqueueRequest{
		Type:      "email",
		QueueName: "email-high",
		Payload:   map[string]any{"to": "b@example.com"},
		Cron:      cron,
		CronTZ:    tz,
	})
	require.NoError(t, err)
	return out
}

func TestCronValidation(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "CronVal", "cronval@example.com")

	// Client surfaces failures as "client: POST /v1/jobs -> 400 {...}".
	bad := func(req client.EnqueueRequest) string {
		t.Helper()
		_, err := c.Enqueue(ctx, req)
		require.Error(t, err)
		return err.Error()
	}

	base := func() client.EnqueueRequest {
		return client.EnqueueRequest{
			Type: "email", QueueName: "email-high",
			Payload: map[string]any{"to": "b@example.com"},
		}
	}

	t.Run("invalid expression is 400", func(t *testing.T) {
		r := base()
		r.Cron = "not a cron"
		require.Contains(t, bad(r), "400")
	})

	t.Run("six-field expression is 400", func(t *testing.T) {
		r := base()
		r.Cron = "0 * * * * *"
		require.Contains(t, bad(r), "400")
	})

	t.Run("unknown timezone is 400", func(t *testing.T) {
		r := base()
		r.Cron = "* * * * *"
		r.CronTZ = "Mars/Olympus"
		require.Contains(t, bad(r), "400")
	})

	t.Run("cron with scheduled_at is 400", func(t *testing.T) {
		r := base()
		r.Cron = "* * * * *"
		r.ScheduledAt = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		require.Contains(t, bad(r), "400")
	})

	t.Run("valid cron creates a scheduled series head", func(t *testing.T) {
		out := enqueueCron(t, c, "*/5 * * * *", "")
		require.Equal(t, "scheduled", out["status"])
		require.NotEmpty(t, out["series_id"])
		require.Equal(t, "*/5 * * * *", out["cron"])
		require.Equal(t, "UTC", out["cron_tz"])
		require.NotEmpty(t, out["scheduled_at"])
		_, hasMsg := out["msg_id"]
		require.False(t, hasMsg, "series head waits for the scheduler")
	})
}

func TestCronSeriesLifecycle(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	a := authedClient(t, fx, "CronA", "cron-a@example.com")
	b := authedClient(t, fx, "CronB", "cron-b@example.com")

	out := enqueueCron(t, a, "* * * * *", "UTC")
	headID, _ := out["job_id"].(string)
	seriesID, _ := out["series_id"].(string)
	require.NotEmpty(t, headID)
	require.NotEmpty(t, seriesID)

	// Rewind the head into the past so the scheduler fires immediately.
	_, err := fx.db.ExecContext(ctx,
		`UPDATE jobs SET scheduled_at = $1 WHERE job_id = $2`,
		time.Now().UTC().Add(-time.Hour), headID)
	require.NoError(t, err)

	t.Run("tick dispatches head and queues follow-up", func(t *testing.T) {
		sched := scheduler.New(fx.jobs, fx.redis, time.Second, 10)
		n, err := sched.Tick(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		got, err := a.GetJob(ctx, headID)
		require.NoError(t, err)
		require.Equal(t, "pending", got["status"])
		require.Equal(t, true, got["is_recurring"])
		require.Equal(t, seriesID, got["series_id"])

		listed, err := a.ListSeries(ctx, seriesID, "", 0, 0)
		require.NoError(t, err)
		members, _ := listed["jobs"].([]any)
		require.Len(t, members, 2)
		require.Equal(t, seriesID, listed["series_id"])
	})

	t.Run("other user sees neither head nor series", func(t *testing.T) {
		_, err := b.GetJob(ctx, headID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")

		listed, err := b.ListSeries(ctx, seriesID, "", 0, 0)
		require.NoError(t, err)
		require.Len(t, listed["jobs"].([]any), 0)

		code, _ := callAPI(t, b, http.MethodDelete, "/v1/jobs/"+headID+"?series=true")
		require.Equal(t, http.StatusNotFound, code)
	})

	t.Run("series cancel stops future occurrences", func(t *testing.T) {
		n, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, int64(1))

		code, body := callAPI(t, a, http.MethodDelete, "/v1/jobs/"+headID+"?series=true")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.Equal(t, seriesID, body["series_id"])
		require.GreaterOrEqual(t, int(body["cancelled_count"].(float64)), 1)

		listed, err := a.ListSeries(ctx, seriesID, "scheduled", 0, 0)
		require.NoError(t, err)
		require.Len(t, listed["jobs"].([]any), 0, "no scheduled members may remain")

		// A later tick dispatches nothing for the cancelled series.
		sched := scheduler.New(fx.jobs, fx.redis, time.Second, 10)
		before, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		_, err = sched.Tick(ctx)
		require.NoError(t, err)
		after, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.Equal(t, before, after)
	})

	t.Run("series_id filter validates and scopes by status", func(t *testing.T) {
		code, _ := callAPI(t, a, http.MethodGet, "/v1/jobs?series_id=not-a-uuid")
		require.Equal(t, http.StatusBadRequest, code)

		code, body := callAPI(t, a, http.MethodGet, "/v1/jobs?series_id="+seriesID+"&status=cancelled")
		require.Equal(t, http.StatusOK, code, "%v", body)
		require.NotEmpty(t, body["jobs"])
	})
}
