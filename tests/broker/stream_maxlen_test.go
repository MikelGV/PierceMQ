package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestStreamMaxLenTrim proves every XADD path carries an approximate MAXLEN
// cap (§10.3 retention): with a tiny cap, hundreds of adds stay bounded
// instead of growing the stream without limit.
func TestStreamMaxLenTrim(t *testing.T) {
	ctx := context.Background()
	store := utils_test.SetUpRedis(t)
	store.MaxLen = 100

	const stream = "stream:queue:email:high"
	const adds = 500
	for i := 0; i < adds; i++ {
		_, err := store.AddJobRefToStream(ctx, stream, task.Job{
			JobID:     uuid.New(),
			Status:    task.JobPending,
			Type:      "email",
			QueueName: "email-high",
			CreatedAt: time.Now().UTC(),
			Payload:   map[string]any{"to": "a@example.com"},
		})
		require.NoError(t, err)
	}

	n, err := store.Conn.XLen(ctx, stream).Result()
	require.NoError(t, err)
	require.Less(t, n, int64(adds), "approximate trim must engage below %d entries", adds)
	require.NotZero(t, n, "trim must keep recent entries, not wipe the stream")

	// Payload survives trimming: newest entry still parses as a job ref.
	entries, err := store.Conn.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	ref, err := task.JobFromFields(entries[0].Values)
	require.NoError(t, err)
	require.Equal(t, "email", ref.Type)
}

// TestStreamMaxLenDefault guards the production cap: an unconfigured store
// resolves to DefaultStreamMaxLen, never uncapped.
func TestStreamMaxLenDefault(t *testing.T) {
	store := utils_test.SetUpRedis(t)
	require.Equal(t, int64(broker.DefaultStreamMaxLen), store.EffectiveMaxLen())
}
