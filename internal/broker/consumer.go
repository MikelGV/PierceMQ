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
				fmt.Printf("XAUTOCLAIM failed: %v", err)
				continue
			}

			if len(claimed) > 0 {
				fmt.Printf("claimed %d stale messages", len(claimed))
			}

			rds.ProcessJobs(ctx, claimed, dispatcher, streamName, groupName)

		default:
			res, err := rds.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Block:    blockTime,
				Count:    35,
			}).Result()

			if err != nil {
				if err == context.Canceled {
					return err
				}
				fmt.Printf("Something went wrong when trying to read the job: %s \n", err)

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
			fmt.Printf("Something went wrong trying to dispatch task: %s /n", err)
			continue
		}

		_, err = rds.AckJob(ctx, streamName, groupName, msg.ID)
		if err != nil {
			fmt.Printf("xAck failed after success id=%s, err=%v/n", msg.ID, err)
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
* Checks what consumers are active
**/
func (rds *RedisStore) CheckLiveGroups(msgId string) error {
	return nil
}

// This is were we claim jobs that haven't been acknwoledged by the ack

func (rds *RedisStore) ClaimFailedJobs(ctx context.Context, streamName, consumerName string) (string, error) {
	return "", nil
}

// This is were we retry jobs that have failed and have been sent back to the broker

func (rds *RedisStore) RetryJobs(ctx context.Context) {
}
