package broker

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	Conn *redis.Client
}

const (
	Email_stream = "tasks:email"
	File_stream  = "tasks:file"
	Exec_stream  = "tasks:exec"

	Email_group = "email-workers"
	File_group  = "file-workers"
	Exec_group  = "exec-workers"
)

func Redis_Connect(rdsUrl string) (*RedisStore, error) {

	opt, err := redis.ParseURL(rdsUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing url: %s\n", err)
		return nil, err
	}

	rds := redis.NewClient(opt)

	if err := rds.Ping(context.Background()).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging redis: %s\n", err)
		return nil, fmt.Errorf("ping redis: %w\n", err)
	}
	streamsAndGroups := map[string]string{
		Email_stream: Email_group,
		File_stream:  File_group,
		Exec_stream:  Exec_group,
	}

	for stream, group := range streamsAndGroups {
		if err = InitConsumerGroupsAndStreams(context.Background(), stream, group, rds); err != nil {
			return nil, fmt.Errorf("Error trying to initialize consumer %w\n", err)
		}
		continue
	}

	return &RedisStore{
		Conn: rds,
	}, nil
}
