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

func Redis_Connect(rdsUrl string) (*RedisStore, error) {

	opt, err := redis.ParseURL(rdsUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing url: %s\n", err)
		return nil, err
	}

	rds := redis.NewClient(opt)

	if err := rds.Ping(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging redis: %s\n", err)
		return nil, fmt.Errorf("%s\n", err)
	}

	return &RedisStore{
		Conn: rds,
	}, nil
}
