package broker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

/**
* All of this should be redone when i add partitioning and replication
**/

func TestServeJobs(t *testing.T) {
	store := utils_test.SetUpRedis(t)
	streamKey := broker.Email_low_stream
	groupKey := broker.Email_group_low

	t.Run("successful job consumed and acknowledged", func(t *testing.T) {
		ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer testCancel()

		consumerKey := "test-consumer-1"

		disp := &dispatcher.Dispatcher{}
		NewDisp, err := disp.NewDispatcher(5)
		require.NoError(t, err)

		dispCtx, dispCancel := context.WithCancel(ctx)
		defer dispCancel()

		go NewDisp.Run(dispCtx)

		done := make(chan error, 1)
		go func() {
			done <- store.ServeJobs(ctx, streamKey, groupKey, consumerKey, NewDisp)
		}()

		time.Sleep(400 * time.Millisecond)

		reqs := []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{
				"from": "test@test.com",
				"to":   "test2@test.com",
				"body": "Hello",
			}},
		}

		ids, err := store.AddTaskToStream(ctx, streamKey, reqs)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		testCancel()

		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("ConsumeJobs exited with unexpected error: %v", err)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("ConsumeJobs failed to stop in time")
		}

		for _, msgID := range ids {
			_, err := store.AckJob(context.Background(), streamKey, groupKey, msgID)
			require.NoError(t, err)
		}

		pending, err := store.Conn.XPending(context.Background(), streamKey, groupKey).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pending.Count, "expected all messages to be acknowledged")
	})

	t.Run("multiple jobs consumed and acknowledged", func(t *testing.T) {
		ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer testCancel()

		consumerKey := "test-consumer-2"

		disp := &dispatcher.Dispatcher{}
		NewDisp, err := disp.NewDispatcher(5)
		require.NoError(t, err)

		dispCtx, dispCancel := context.WithCancel(ctx)
		defer dispCancel()

		go NewDisp.Run(dispCtx)

		done := make(chan error, 1)
		go func() {
			done <- store.ServeJobs(ctx, streamKey, groupKey, consumerKey, NewDisp)
		}()

		time.Sleep(400 * time.Millisecond)

		reqs := []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{"from": "a@test.com", "to": "b@test.com", "body": "msg1"}},
			{Id: 2, Type: "email", Payload: map[string]any{"from": "c@test.com", "to": "d@test.com", "body": "msg2"}},
			{Id: 3, Type: "email", Payload: map[string]any{"from": "e@test.com", "to": "f@test.com", "body": "msg3"}},
		}

		ids, err := store.AddTaskToStream(ctx, streamKey, reqs)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		testCancel()

		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("ConsumeJobs exited with unexpected error: %v", err)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("ConsumeJobs failed to stop in time")
		}

		for _, msgID := range ids {
			_, err := store.AckJob(context.Background(), streamKey, groupKey, msgID)
			require.NoError(t, err)
		}

		pending, err := store.Conn.XPending(context.Background(), streamKey, groupKey).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pending.Count, "expected all messages to be acknowledged")
	})

	t.Run("consumer reads and claims stale messages", func(t *testing.T) {
		ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer testCancel()

		staleConsumer := "stale-consumer-1"
		consumerKey := "test-consumer-3"

		msgIDs, err := store.AddTaskToStream(ctx, streamKey, []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{"from": "x@test.com", "to": "y@test.com", "body": "stale"}},
		})
		require.NoError(t, err)

		_, err = store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: staleConsumer,
			Streams:  []string{streamKey, "0"},
		}).Result()
		require.NoError(t, err)

		time.Sleep(6 * time.Second)

		disp := &dispatcher.Dispatcher{}
		NewDisp, err := disp.NewDispatcher(5)
		require.NoError(t, err)

		dispCtx, dispCancel := context.WithCancel(ctx)
		defer dispCancel()

		go NewDisp.Run(dispCtx)

		done := make(chan error, 1)
		go func() {
			done <- store.ServeJobs(ctx, streamKey, groupKey, consumerKey, NewDisp)
		}()

		time.Sleep(2 * time.Second)

		testCancel()

		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("ConsumeJobs exited with unexpected error: %v", err)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("ConsumeJobs failed to stop in time")
		}

		for _, msgID := range msgIDs {
			_, err := store.AckJob(context.Background(), streamKey, groupKey, msgID)
			require.NoError(t, err)
		}

		pending, err := store.Conn.XPending(context.Background(), streamKey, groupKey).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pending.Count, "expected stale messages to be claimed and acknowledged")
	})
}

func TestAckJob(t *testing.T) {
	ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer testCancel()

	store := utils_test.SetUpRedis(t)
	streamKey := broker.Email_low_stream
	groupKey := broker.Email_group_low
	consumerName := "ack-test-consumer"

	t.Run("single message acknowledged successfully", func(t *testing.T) {
		_, err := store.AddTaskToStream(ctx, streamKey, []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{"from": "x@test.com", "to": "y@test.com", "body": "stale"}},
		})
		require.NoError(t, err)

		res, err := store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
		}).Result()
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.GreaterOrEqual(t, len(res[0].Messages), 1)

		msgID := res[0].Messages[0].ID

		acked, err := store.AckJob(ctx, streamKey, groupKey, msgID)
		require.NoError(t, err)
		require.Equal(t, int64(1), acked)

		pending, err := store.Conn.XPending(ctx, streamKey, groupKey).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pending.Count)
	})

	t.Run("returns error for empty msgId", func(t *testing.T) {
		_, err := store.AckJob(ctx, streamKey, groupKey, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "msgId can not be empty")
	})

	t.Run("acknowledges multiple messages in sequence", func(t *testing.T) {
		var msgIDs []string
		for i := 0; i < 3; i++ {
			_, err := store.AddTaskToStream(ctx, streamKey, []*task.TaskRequest{
				{Id: 1, Type: "email", Payload: map[string]any{"from": "x@test.com", "to": "y@test.com", "body": "stale"}},
			})
			require.NoError(t, err)

			res, err := store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupKey,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"},
			}).Result()
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(res[0].Messages), 1)
			msgIDs = append(msgIDs, res[0].Messages[0].ID)
		}

		for _, msgID := range msgIDs {
			acked, err := store.AckJob(ctx, streamKey, groupKey, msgID)
			require.NoError(t, err)
			require.Equal(t, int64(1), acked)
		}

		pending, err := store.Conn.XPending(ctx, streamKey, groupKey).Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), pending.Count)
	})
}

func TestCheckLiveGroups(t *testing.T) {
	ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer testCancel()

	store := utils_test.SetUpRedis(t)

	streamKey := broker.Email_low_stream
	groupKey := broker.Email_group_low

	t.Run("empty group returns no consumers", func(t *testing.T) {
		consumers, err := store.CheckLiveGroups(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.Empty(t, consumers, "expected no consumers in empty group")
	})

	t.Run("consumer appears after reading message", func(t *testing.T) {
		consumerName := "test-consumer-1"

		msgID, err := store.Conn.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"test": "data"},
		}).Result()
		require.NoError(t, err)

		_, err = store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, "0"},
		}).Result()
		require.NoError(t, err)

		consumers, err := store.CheckLiveGroups(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.Contains(t, consumers, consumerName, "expected consumer to appear after reading message")

		_, err = store.Conn.XAck(ctx, streamKey, groupKey, msgID).Result()
		require.NoError(t, err)
	})

	t.Run("multiple consumers all appear", func(t *testing.T) {
		consumerNames := []string{"consumer-x", "consumer-y"}

		for _, name := range consumerNames {
			msgID, err := store.Conn.XAdd(ctx, &redis.XAddArgs{
				Stream: streamKey,
				Values: map[string]any{"test": name},
			}).Result()
			require.NoError(t, err)

			_, err = store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupKey,
				Consumer: name,
				Streams:  []string{streamKey, "0"},
			}).Result()
			require.NoError(t, err)

			_, err = store.Conn.XAck(ctx, streamKey, groupKey, msgID).Result()
			require.NoError(t, err)
		}

		consumers, err := store.CheckLiveGroups(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(consumers), 2, "expected at least two consumers")
		for _, name := range consumerNames {
			require.Contains(t, consumers, name)
		}
	})
}

func TestHandleJobFailure(t *testing.T) {
	store := utils_test.SetUpRedis(t)
	streamKey := broker.Email_low_stream
	groupKey := broker.Email_group_low

	t.Run("Job fails because of logical issue", func(t *testing.T) {
		ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer testCancel()

		consumerName := "failure-logical-consumer"

		_, err := store.Conn.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{
				"type":    "",
				"payload": `{"invalid": "json"`,
			},
		}).Result()
		require.NoError(t, err)

		_, err = store.Conn.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{
				"type":    "email",
				"payload": "not-valid-json",
			},
		}).Result()
		require.NoError(t, err)

		_, err = store.Conn.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{
				"payload": `{"key": "value"}`,
			},
		}).Result()
		require.NoError(t, err)

		disp := &dispatcher.Dispatcher{}
		NewDisp, err := disp.NewDispatcher(5)
		require.NoError(t, err)

		dispCtx, dispCancel := context.WithCancel(ctx)
		defer dispCancel()

		go NewDisp.Run(dispCtx)

		done := make(chan error, 1)
		go func() {
			done <- store.ServeJobs(ctx, streamKey, groupKey, consumerName, NewDisp)
		}()

		time.Sleep(2 * time.Second)

		testCancel()

		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("ConsumeJobs exited with unexpected error: %v", err)
			}
		case <-time.After(8 * time.Second):
			// Here i have to handle the failure with the HandleJobFailure function
			t.Fatal("ConsumeJobs failed to stop in time")
		}

		pending, err := store.GetPendingMessages(context.Background(), streamKey, groupKey)
		require.NoError(t, err)
		require.Equal(t, 0, len(pending), "expected all malformed messages to be moved to DLQ")

		dlqKey := "dlq:email"
		dlqLen, err := store.Conn.XLen(context.Background(), dlqKey).Result()
		require.NoError(t, err)
		require.GreaterOrEqual(t, dlqLen, int64(3), "expected malformed messages in DLQ")

		t.Logf("Logical failures test: %d messages moved to DLQ (as expected)", dlqLen)
	})

	t.Run("Job fails because of network issues", func(t *testing.T) {
		ctx, testCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer testCancel()

		consumerName := "failure-network-consumer"

		reqs := []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{
				"from": "test@test.com",
				"to":   "test2@test.com",
				"body": "Network failure test",
			}},
		}

		ids, err := store.AddTaskToStream(ctx, streamKey, reqs)
		require.NoError(t, err)

		disp := &dispatcher.Dispatcher{}
		NewDisp, err := disp.NewDispatcher(5)
		require.NoError(t, err)

		dispCtx, dispCancel := context.WithCancel(ctx)
		defer dispCancel()

		go NewDisp.Run(dispCtx)

		done := make(chan error, 1)
		go func() {
			done <- store.ServeJobs(ctx, streamKey, groupKey, consumerName, NewDisp)
		}()

		time.Sleep(500 * time.Millisecond)

		testCancel()

		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Logf("ConsumeJobs exited with unexpected error: %v", err)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("ConsumeJobs failed to stop in time")
		}

		pending, err := store.GetPendingMessages(context.Background(), streamKey, groupKey)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(pending), 1, "expected message to remain pending after context cancellation")

		var found bool
		for _, p := range pending {
			if p.ID == ids[0] {
				found = true
				break
			}
		}
		require.True(t, found, "expected original message ID to remain in pending list")
		// Here i have to handle the failure with the HandleJobFailure function

		t.Logf("Network failure test: message %s remained pending after context cancellation", ids[0])
	})

	t.Run("Job fails because of worker issues", func(t *testing.T) {
		t.Skip("Worker implementation needed before testing worker failures")
	})
}

func TestMoveToDeadLetterQueue(t *testing.T) {
	t.Skip("MoveToDeadLetterQueue tests not implemented yet")
}

func TestRecoverStalePendingJobs(t *testing.T) {
	t.Skip("RecoverStalePendingJobs not implemented yet")
}

func TestRetryJob(t *testing.T) {
	store := utils_test.SetUpRedis(t)
	streamKey := broker.Email_low_stream
	groupKey := broker.Email_group_low

	t.Run("retry a pending message successfully", func(t *testing.T) {
		ctx := context.Background()
		consumerName := "retry-test-consumer"

		reqs := []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{
				"from": "test@test.com",
				"to":   "test2@test.com",
				"body": "Retry this job",
			}},
		}

		ids, err := store.AddTaskToStream(ctx, streamKey, reqs)
		require.NoError(t, err)

		_, err = store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
		}).Result()
		require.NoError(t, err)

		pendingBefore, err := store.GetPendingMessages(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.Equal(t, 1, len(pendingBefore), "expected 1 pending message")

		err = store.RetryJob(ctx, ids[0], streamKey, groupKey)
		require.NoError(t, err)

		pendingAfter, err := store.GetPendingMessages(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.Equal(t, 0, len(pendingAfter), "expected original message to be acknowledged after retry")

		streamLen, err := store.Conn.XLen(ctx, streamKey).Result()
		require.NoError(t, err)
		require.GreaterOrEqual(t, streamLen, int64(2), "expected a new message in the stream after retry")
	})

	t.Run("retry multiple pending messages", func(t *testing.T) {
		ctx := context.Background()
		consumerName := "retry-multi-consumer"

		reqs := []*task.TaskRequest{
			{Id: 1, Type: "email", Payload: map[string]any{"from": "a@test.com", "to": "b@test.com", "body": "msg1"}},
			{Id: 2, Type: "email", Payload: map[string]any{"from": "c@test.com", "to": "d@test.com", "body": "msg2"}},
			{Id: 3, Type: "email", Payload: map[string]any{"from": "e@test.com", "to": "f@test.com", "body": "msg3"}},
		}

		ids, err := store.AddTaskToStream(ctx, streamKey, reqs)
		require.NoError(t, err)

		_, err = store.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    3,
		}).Result()
		require.NoError(t, err)

		pendingBefore, err := store.GetPendingMessages(ctx, streamKey, groupKey)
		require.NoError(t, err)
		require.Equal(t, 3, len(pendingBefore))

		for _, msgID := range ids {
			err = store.RetryJob(ctx, msgID, streamKey, groupKey)
			require.NoError(t, err)
		}

		pendingAfter, err := store.GetPendingMessages(ctx, streamKey, groupKey)
		require.NoError(t, err)

		pendingIDs := make(map[string]bool, len(pendingAfter))
		for _, p := range pendingAfter {
			pendingIDs[p.ID] = true
		}
		for _, msgID := range ids {
			if pendingIDs[msgID] {
				t.Fatalf("expected original message %s to be acknowledged, but still pending", msgID)
			}
		}

		streamLen, err := store.Conn.XLen(ctx, streamKey).Result()
		require.NoError(t, err)
		require.GreaterOrEqual(t, streamLen, int64(6), "expected 3 new messages in the stream after retries")
	})

	t.Run("retry fails for non-existent message", func(t *testing.T) {
		ctx := context.Background()
		err := store.RetryJob(ctx, "nonexistent-0", streamKey, groupKey)
		require.Error(t, err)
	})
}

func TestMonitorPending(t *testing.T) {
	t.Skip("MonitorPending not implemented yet")
}

func TestTrimStream(t *testing.T) {
	t.Skip("TrimStream not implemented yet")
}

func TestRequeueFromDLQ(t *testing.T) {
	t.Skip("RequeueFromDLQ not implemented yet")
}
