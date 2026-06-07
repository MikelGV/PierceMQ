package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

const (
	claimInIdl     = 5 * time.Second
	claimBatchSize = 35
	blockTime      = 100 * time.Millisecond
)

/**
* ConsumeJob function consumes the job that has been choosen
**/

func (rds *RedisStore) ConsumeJobs(ctx context.Context, streamName, groupName, consumerName string, dispatcher *dispatcher.Dispatcher) error {
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

func (rds *RedisStore) ProcessJobs(ctx context.Context, Messages []redis.XMessage, dispatcher *dispatcher.Dispatcher, streamName, groupName string) {
	for _, msg := range Messages {

		taskreq, err := task.FromFields(msg.Values)
		if err != nil {
			fmt.Printf("Something went wrong: %s /n", err)
			continue
		}

		job := &task.Job{
			ID:          msg.ID,
			TYPE:        taskreq.Type,
			PAYLOAD:     taskreq.Payload,
			ATTEMPT:     0,
			MAX_RETRIES: 3,
		}

		err = dispatcher.DispatchTask(ctx, job)
		if err != nil {
			fmt.Printf("Something went wrong trying to dispatch task: %s \n", err)
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

/**
* I might rework the part for retry and handlejobfailiure after I decide in the
* system design how to handle it correctly so for now I will not keep working on int
**/

// This is function encapsulates both recovering and retrying jobs
func (rds *RedisStore) HandleJobFailiure(ctx context.Context) {
	/**
	* So this function is for handling the retry and recover functions
	**/
}

// This is the function with the logic for retrying jobs
func (rds *RedisStore) RetryJob(ctx context.Context, ids, streamkey string) error {
	return nil
}

func (rds *RedisStore) MoveToDeadLetterQueue(ctx context.Context, streamName, consumerName string) (string, error) {
	return "", nil
}

// This is were we claim jobs that haven't been acknwoledged by the ack

func (rds *RedisStore) RecoverStalePendingJobs(ctx context.Context, streamName, consumerName string) (string, error) {
	return "", nil
}

// Here we get how many jobs are in pending
func (rds *RedisStore) MonitorPending(ctx context.Context) {}

func (rds *RedisStore) TrimStream(ctx context.Context) {}

// This is were we requeue jobs from the dead letter queue
func (rds *RedisStore) RequeueFromDLQ(ctx context.Context) {}
