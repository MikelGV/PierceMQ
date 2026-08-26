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
	Email_high_stream = "stream:queue:email:high"
	Email_low_stream  = "stream:queue:email:low"
	File_high_stream  = "stream:queue:file_processing:high"
	File_low_stream   = "stream:queue:file_processing:low"
	Exec_high_stream  = "stream:queue:exec_processing:high"
	Exec_low_stream   = "stream:queue:exec_processing:low"

	Email_group_high = "email-workers-high"
	Email_group_low  = "email-workers-low"
	File_group_high  = "file_processing-workers-high"
	File_group_low   = "file_processing-workers-low"
	Exec_group_high  = "exec-workers-workers-high"
	Exec_group_low   = "exec-workers-workers-low"
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
		Email_high_stream: Email_group_high,
		Email_low_stream:  Email_group_low,
		File_high_stream:  File_group_high,
		File_low_stream:   File_group_low,
		Exec_high_stream:  Exec_group_high,
		Exec_low_stream:   Exec_group_low,
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
