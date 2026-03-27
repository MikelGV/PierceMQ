package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
)

func TestConsumeJobs(t *testing.T) {
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)

	defer cancel()

	store := utils_test.SetUpRedis(t)

	var streamKey = broker.Email_stream
	var groupKey = broker.Email_group
	var consumerKey = "test-consumer"

	disp := &dispatcher.Dispatcher{}
	NewDisp, err := disp.NewDispatcher(5)
	require.NoError(t, err)

	dispCtx, dispCancel := context.WithCancel(ctx)
	defer dispCancel()

	go NewDisp.Run(dispCtx)

	done := make(chan error, 1)

	go func() {
		done <- store.ConsumeJobs(ctx, streamKey, groupKey, consumerKey, NewDisp)
	}()

	reqs := []*task.TaskRequest{
		{Id: 1, Type: "email", Payload: map[string]any{
			"from": "test@test.com",
			"to":   "test2@test.com",
			"body": "Hello mate this is test how you doing ma boy",
		}},
		{Id: 2, Type: "email", Payload: map[string]any{
			"from": "test2@test.com",
			"to":   "test@test.com",
			"body": "Hello mate this is test2 how you doing ma boy",
		}},
	}

	_, err = store.AddTaskToStream(ctx, streamKey, reqs)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(8 * time.Second):
	}

	pending, err := store.Conn.XPending(ctx, streamKey, groupKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), pending.Count, "expected all messages to be acknowledged, but found pending messages")

	t.Logf("Test completed successfully: %d jobs produced and processed", len(reqs))

}
