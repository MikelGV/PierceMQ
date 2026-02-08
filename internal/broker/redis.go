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

func Redis_Connect() (*RedisStore, error) {

	opt, err := redis.ParseURL("redis://1234567890ca@localhost:6379/0")

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
