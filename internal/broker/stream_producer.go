package broker

import (
	"context"
	"fmt"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

//var rds RedisStore

// Here we create a consumer group that will handle multilpe messages in a stream
func InitConsumerGroupsAndStreams(ctx context.Context, streamName, groupName string, rds *redis.Client) error {
	if ctx == nil {
		ctx = context.Background()
	}

	err := rds.XGroupCreateMkStream(context.Background(), streamName, groupName, "0").Err()

	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("failed to initialize stream %q with group %q: %w\n", streamName, groupName, err)
	}

	return nil

}

// Here we add a task to a stream
func (rds *RedisStore) AddTaskToStream(ctx context.Context, streamName string, tasks []*task.TaskRequest) ([]string, error) {
	pipeline := rds.Conn.Pipeline()

	var cmds []*redis.StringCmd

	for _, task := range tasks {
		cmd := pipeline.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: task.ToFields(),
			ID:     "*",
		})

		cmds = append(cmds, cmd)
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		return nil, fmt.Errorf("An Error Occurred while trying to exec pipeline: %v \n", err)
	}

	var ids []string
	for _, cmd := range cmds {

		id, err := cmd.Result()
		if err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	return ids, nil
}
