package broker

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

/**
* Okay i need a way to get the consumer name and stream name so when the worker
* wants to consume the task i think i should have a request struct containing
* the stream name, the consumer name, and maybe the task id?
**/

func ConsumeJob(ctx context.Context, streamName, groupName, consumerName string) (string, error) {
	var rds *redis.Client

	pipe := rds.Pipeline()

	var cmds *redis.XStreamSliceCmd

	if err := pipe.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
	}); err != nil {

		return "", fmt.Errorf("Something went wrong when trying to process the job: %s /n", err)
	}
	return "", nil
}

func ClaimPendingJobs(ctx context.Context, streamName, consumerName string) (string, error) {
	return "", nil
}

func RetryJobs(ctx context.Context) {}
