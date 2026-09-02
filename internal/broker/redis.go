package broker

import (
	"context"
	"fmt"
	"os"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	Conn *redis.Client
}

// Re-export queue constants for backward compatibility; canonical source is internal/queue.
const (
	Email_high_stream = queue.EmailHighStream
	Email_low_stream  = queue.EmailLowStream
	File_high_stream  = queue.FileHighStream
	File_low_stream   = queue.FileLowStream
	Exec_high_stream  = queue.ExecHighStream
	Exec_low_stream   = queue.ExecLowStream

	Email_group_high = queue.EmailGroupHigh
	Email_group_low  = queue.EmailGroupLow
	File_group_high  = queue.FileGroupHigh
	File_group_low   = queue.FileGroupLow
	Exec_group_high  = queue.ExecGroupHigh
	Exec_group_low   = queue.ExecGroupLow
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
