package api_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestOwnerIsolation verifies tenant isolation: jobs created by user A are
// invisible to user B (404 on get/events/retry/cancel, excluded from list
// and stats), while user A retains full access.
func TestOwnerIsolation(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	a := authedClient(t, fx, "OwnerA", "owner-a@example.com")
	b := authedClient(t, fx, "OwnerB", "owner-b@example.com")

	id := enqueueEmail(t, a, "email-high")

	t.Run("B cannot read A's job", func(t *testing.T) {
		_, err := b.GetJob(ctx, id)
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")

		_, err = b.GetJobEvents(ctx, id)
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})

	t.Run("B cannot cancel or retry A's job", func(t *testing.T) {
		code, _ := callAPI(t, b, http.MethodDelete, "/v1/jobs/"+id)
		require.Equal(t, http.StatusNotFound, code)

		code, _ = callAPI(t, b, http.MethodPost, "/v1/jobs/"+id+"/retry")
		require.Equal(t, http.StatusNotFound, code)
	})

	t.Run("B list and stats exclude A's job", func(t *testing.T) {
		listed, err := b.ListJobs(ctx, "", 0, 0)
		require.NoError(t, err)
		require.Len(t, listed["jobs"].([]any), 0)

		stats, err := b.Stats(ctx)
		require.NoError(t, err)
		require.Equal(t, float64(0), stats["pending"])
	})

	t.Run("A retains full access", func(t *testing.T) {
		got, err := a.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "pending", got["status"])

		listed, err := a.ListJobs(ctx, "", 0, 0)
		require.NoError(t, err)
		require.Len(t, listed["jobs"].([]any), 1)

		stats, err := a.Stats(ctx)
		require.NoError(t, err)
		require.Equal(t, float64(1), stats["pending"])

		code, body := callAPI(t, a, http.MethodDelete, "/v1/jobs/"+id)
		require.Equal(t, http.StatusOK, code, "%v", body)
	})

	t.Run("unknown job is 404 for owner too", func(t *testing.T) {
		_, err := a.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})

	t.Run("failed job retry is owner-scoped", func(t *testing.T) {
		id2 := enqueueEmail(t, a, "email-high")
		forceFail(t, fx, id2)

		code, _ := callAPI(t, b, http.MethodPost, "/v1/jobs/"+id2+"/retry")
		require.Equal(t, http.StatusNotFound, code)

		retried, err := a.Retry(ctx, id2)
		require.NoError(t, err)
		require.Equal(t, "pending", retried["status"])
	})
}
