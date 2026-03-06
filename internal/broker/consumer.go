package broker

import (
	"context"
	"fmt"

	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

/**
* ConsumeJob function consumes the job that has been choosen
**/

func (rds *RedisStore) ConsumeJobs(ctx context.Context, streamName, groupName, consumerName string, dispatcher *dispatcher.Dispatcher) error {
	for {

		res, err := rds.Conn.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Block:    0,
			Count:    50,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				return err
			}
			fmt.Printf("Something went wrong when trying to read the job: %s /n", err)

			continue
		}

		for _, streams := range res {
			for _, msg := range streams.Messages {

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

			}
		}
	}
}

// This is were we claim jobs that haven't been acknwoledged by the ack

func (rds *RedisStore) ClaimPendingJobs(ctx context.Context, streamName, consumerName string) (string, error) {
	return "", nil
}

// This is were we retry jobs that have failed and have been sent back to the broker

func (rds *RedisStore) RetryJobs(ctx context.Context) {}
