package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

const (
	claimInIdl     = 5 * time.Second
	claimBatchSize = 35
	blockTime      = 100 * time.Millisecond
	maxRetries     = 3
)

/**
* ServeJobs reads messages from a Redis stream and feeds them as jobs to workers via the dispatcher.
**/

func (rds *RedisStore) ServeJobs(ctx context.Context, streamName, groupName, consumerName string, dispatcher *dispatcher.Dispatcher) error {
	ticker := time.NewTicker(claimInIdl / 5)
	defer ticker.Stop()

	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

			claimed, _, err := rds.Conn.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Consumer: consumerName,
				Group:    groupName,
				MinIdle:  claimInIdl,
				Count:    claimBatchSize,
				Start:    "0-0",
			}).Result()

			if err != nil && err != redis.Nil {
				fmt.Printf("XAUTOCLAIM failed: %v \n", err)
				continue
			}

			if len(claimed) > 0 {
				fmt.Printf("claimed %d stale messages", len(claimed))
				rds.ProcessJobs(ctx, claimed, dispatcher, streamName, groupName)
			}

		default:
			res, err := rds.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Block:    blockTime,
				Count:    35,
			}).Result()

			if err == redis.Nil || len(res) == 0 {
				continue
			}

			if err != nil {
				if err == context.Canceled {
					return err
				}
				fmt.Printf("XReadGroup failed: %v \n", err)

				continue
			}

			for _, streams := range res {
				rds.ProcessJobs(ctx, streams.Messages, dispatcher, streamName, groupName)
			}

		}

	}
}

func (rds *RedisStore) ProcessJobs(ctx context.Context, messages []redis.XMessage, d *dispatcher.Dispatcher, streamName, groupName string) {
	for _, msg := range messages {
		taskreq, err := task.FromFields(msg.Values)
		if err != nil {
			fmt.Printf("Malformed message %s: %v — moving to DLQ\n", msg.ID, err)
			rds.MoveToDeadLetterQueue(ctx, msg.ID, streamName, groupName, "malformed message: "+err.Error())
			continue
		}

		job := &task.Job{
			ID:          msg.ID,
			TYPE:        taskreq.Type,
			PAYLOAD:     taskreq.Payload,
			ATTEMPT:     taskreq.Attempt,
			MAX_RETRIES: maxRetries,
		}

		err = d.DispatchTask(ctx, job)
		if err != nil {
			fmt.Printf("Failed to dispatch job %s: %v\n", msg.ID, err)
			rds.HandleJobFailure(ctx, msg.ID, streamName, groupName, taskreq.Attempt)
			continue
		}

	}

}

/**
* Acknwoledge job
**/
func (rds *RedisStore) AckJob(ctx context.Context, streamName, groupName, msgId string) (int64, error) {

	if msgId == "" {
		return 0, fmt.Errorf("msgId can not be empty")
	}

	acked, err := rds.Conn.XAck(ctx, streamName, groupName, msgId).Result()
	if err != nil {
		return 0, fmt.Errorf("xAck %s %s %s failed: %w\n", streamName, groupName, msgId, err)
	}

	return acked, nil
}

/**
* Checks what consumer groups are active
**/
func (rds *RedisStore) CheckLiveGroups(ctx context.Context, streamName, groupName string) ([]string, error) {
	consumers, err := rds.Conn.XInfoConsumers(ctx, streamName, groupName).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get consumers for stream %s, group %s: %w", streamName, groupName, err)
	}

	names := make([]string, len(consumers))
	for i, c := range consumers {
		names[i] = c.Name
	}
	return names, nil
}

func (rds *RedisStore) GetPendingMessages(ctx context.Context, streamName, groupName string) ([]redis.XPendingExt, error) {
	entries, err := rds.Conn.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get pending details: %w", err)
	}

	return entries, nil
}

func (rds *RedisStore) HandleJobFailure(ctx context.Context, msgID, streamName, groupName string, attempt int) error {
	if attempt < maxRetries {
		return rds.RetryJob(ctx, msgID, streamName, groupName)
	}
	return rds.MoveToDeadLetterQueue(ctx, msgID, streamName, groupName, "max retries exceeded")
}

func (rds *RedisStore) RetryJob(ctx context.Context, msgID, streamName, groupName string) error {
	msgs, err := rds.Conn.XRange(ctx, streamName, msgID, msgID).Result()
	if err != nil {
		return fmt.Errorf("failed to read message %s for retry: %w", msgID, err)
	}
	if len(msgs) == 0 {
		return fmt.Errorf("message %s not found in stream %s", msgID, streamName)
	}

	values := msgs[0].Values
	attempt := getAttempt(values)
	values["attempt"] = strconv.Itoa(attempt + 1)

	newID, err := rds.Conn.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
		ID:     "*",
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to requeue message %s: %w", msgID, err)
	}

	if _, err := rds.AckJob(ctx, streamName, groupName, msgID); err != nil {
		return fmt.Errorf("failed to ack original message %s after retry: %w", msgID, err)
	}

	fmt.Printf("Retried message %s as %s (attempt %d)\n", msgID, newID, attempt+1)
	return nil
}

func (rds *RedisStore) MoveToDeadLetterQueue(ctx context.Context, msgID, streamName, groupName, reason string) error {
	msgs, err := rds.Conn.XRange(ctx, streamName, msgID, msgID).Result()
	if err != nil {
		return fmt.Errorf("failed to read message %s for DLQ: %w", msgID, err)
	}
	if len(msgs) == 0 {
		return fmt.Errorf("message %s not found in stream %s", msgID, streamName)
	}

	values := msgs[0].Values
	values["_dlq_reason"] = reason
	values["_dlq_time"] = strconv.FormatInt(time.Now().Unix(), 10)
	values["_original_stream"] = streamName

	dlqName := dlqStream(streamName)
	if _, err := rds.Conn.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqName,
		Values: values,
		ID:     "*",
	}).Result(); err != nil {
		return fmt.Errorf("failed to move message %s to DLQ %s: %w", msgID, dlqName, err)
	}

	if _, err := rds.AckJob(ctx, streamName, groupName, msgID); err != nil {
		return fmt.Errorf("failed to ack message %s after moving to DLQ: %w", msgID, err)
	}

	fmt.Printf("Moved message %s to DLQ %s (reason: %s)\n", msgID, dlqName, reason)
	return nil
}

func getAttempt(values map[string]any) int {
	if raw, ok := values["attempt"]; ok {
		switch v := raw.(type) {
		case string:
			n, err := strconv.Atoi(v)
			if err == nil {
				return n
			}
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return 0
}

func dlqStream(streamName string) string {
	return strings.Replace(streamName, "tasks:", "dlq:", 1)
}

/**
// Here we get how many jobs are in pending
func (rds *RedisStore) MonitorPending(ctx context.Context) {}

func (rds *RedisStore) TrimStream(ctx context.Context) {}

// This is were we requeue jobs from the dead letter queue
func (rds *RedisStore) RequeueFromDLQ(ctx context.Context) {}
**/
