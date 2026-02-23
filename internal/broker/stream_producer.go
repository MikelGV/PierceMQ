package broker

import (
	"context"
	"fmt"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

func (rds *RedisStore) ProduceRedisStream(streamName string, taskValues task.FileProcessing_Payload) (string, error) {
	/**
	* I think this should work but i need to change the taskValues so that it accepts
	* multiple payloads or handle the values differently maybe instead of taking the values from the
	* payload i could take the values from the db or something like that maybe
	* I could have a custom struct here that gets populated by the other payloads
	**/
	rs1, err := rds.Conn.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: taskValues,
	}).Result()

	if err != nil {
		return "", err
	}

	fmt.Println(rs1)

	return "", nil
}
