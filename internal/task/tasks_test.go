package task

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJobFieldsRoundTrip(t *testing.T) {
	id := uuid.New()
	created := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	in := Job{
		JobID:          id,
		Status:         JobPending,
		Type:           "email",
		QueueName:      "email-high",
		Priority:       1,
		AttemptCount:   2,
		MaxRetry:       3,
		IdempotencyKey: sql.NullString{String: "k-1", Valid: true},
		CreatedAt:      created,
		Payload:        map[string]any{"to": "a@example.com"},
	}

	fields := in.ToJobFields()
	require.Equal(t, id.String(), fields["job_id"])
	require.Equal(t, "email", fields["job_type"])
	require.Equal(t, "pending", fields["status"])

	out, err := JobFromFields(fields)
	require.NoError(t, err)
	require.Equal(t, id, out.JobID)
	require.Equal(t, "email", out.Type)
	require.Equal(t, JobPending, out.Status)
	require.Equal(t, "email-high", out.QueueName)
	require.Equal(t, int16(2), out.AttemptCount)
	require.Equal(t, int16(3), out.MaxRetry)
	require.Equal(t, "k-1", out.IdempotencyKey.String)
	require.True(t, out.CreatedAt.Equal(created))
	require.NotNil(t, out.Payload)
}

func TestJobFromFieldsRequiresIdentity(t *testing.T) {
	_, err := JobFromFields(map[string]any{"job_type": "email"})
	require.Error(t, err, "missing job_id must fail so callers fall back to legacy parse")

	_, err = JobFromFields(map[string]any{"job_id": uuid.New().String()})
	require.Error(t, err, "missing job_type must fail")
}
