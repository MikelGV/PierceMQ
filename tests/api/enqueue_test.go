package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func authedClient(t *testing.T, fx *apiFixture, name, email string) *client.Client {
	t.Helper()
	ctx := context.Background()
	c := &client.Client{BaseURL: fx.server.URL}
	_, err := c.Register(ctx, name, email, "password123")
	require.NoError(t, err)
	_, err = c.Login(ctx, email, "password123")
	require.NoError(t, err)
	return c
}

func TestEnqueueImmediate(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Enq", "enq@example.com")

	t.Run("enqueue lands in PG and in the stream", func(t *testing.T) {
		out, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type:      "email",
			QueueName: "email-high",
			Payload:   map[string]any{"from": "a@example.com", "to": "b@example.com"},
		})
		require.NoError(t, err)
		jobID, ok := out["job_id"].(string)
		require.True(t, ok && jobID != "", "response must carry job_id: %v", out)
		require.Equal(t, "pending", out["status"])
		require.Equal(t, queue.EmailHighStream, out["stream"])
		require.NotEmpty(t, out["msg_id"], "DB-first write must also XADD")

		// DB side: row exists and is readable.
		parsed, err := uuid.Parse(jobID)
		require.NoError(t, err)
		row, err := fx.jobs.GetJobByID(ctx, parsed)
		require.NoError(t, err)
		require.Equal(t, "email", row.Type)

		// Stream side: entry present.
		n, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, int64(1))

		// Read-back via API + events timeline.
		got, err := c.GetJob(ctx, jobID)
		require.NoError(t, err)
		require.Equal(t, "pending", got["status"])

		events, err := c.GetJobEvents(ctx, jobID)
		require.NoError(t, err)
		list, ok := events["events"].([]any)
		require.True(t, ok && len(list) >= 1, "expected initial event: %v", events)
	})

	t.Run("low priority routes to the low stream", func(t *testing.T) {
		out, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type:      "email",
			QueueName: "email-low",
			Payload:   map[string]any{"from": "a@example.com", "to": "b@example.com"},
		})
		require.NoError(t, err)
		require.Equal(t, queue.EmailLowStream, out["stream"])
	})
}

func TestEnqueueScheduled(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Sched", "sched@example.com")

	t.Run("future job is stored but not dispatched", func(t *testing.T) {
		future := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
		before, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)

		out, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type:        "email",
			QueueName:   "email-high",
			Payload:     map[string]any{"from": "a@example.com", "to": "b@example.com"},
			ScheduledAt: future,
		})
		require.NoError(t, err)
		require.Equal(t, "scheduled", out["status"])
		_, hasMsg := out["msg_id"]
		require.False(t, hasMsg, "scheduled jobs must skip XADD until the scheduler runs")

		after, err := fx.redis.Conn.XLen(ctx, queue.EmailHighStream).Result()
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}

func TestEnqueueValidation(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := authedClient(t, fx, "Val", "val@example.com")

	t.Run("unknown type is 400", func(t *testing.T) {
		_, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type: "video", QueueName: "video-high",
			Payload: map[string]any{"url": "x"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "400")
	})

	t.Run("missing payload is 400", func(t *testing.T) {
		_, err := c.Enqueue(ctx, client.EnqueueRequest{Type: "email", QueueName: "email-high"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "400")
	})

	t.Run("bad scheduled_at is 400", func(t *testing.T) {
		_, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type: "email", QueueName: "email-high",
			Payload:     map[string]any{"from": "a", "to": "b"},
			ScheduledAt: "not-a-time",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "400")
	})

	t.Run("unauthenticated is 401", func(t *testing.T) {
		anon := &client.Client{BaseURL: fx.server.URL}
		_, err := anon.Enqueue(ctx, client.EnqueueRequest{
			Type: "email", QueueName: "email-high",
			Payload: map[string]any{"from": "a", "to": "b"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")

		_, err = anon.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")
	})

	t.Run("unknown job id is 404", func(t *testing.T) {
		_, err := c.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})
}
