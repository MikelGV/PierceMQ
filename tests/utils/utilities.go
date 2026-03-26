package utils_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Email_stream = "tasks:email"
	File_stream  = "tasks:file"
	Exec_stream  = "tasks:exec"

	Email_group = "email-workers"
	File_group  = "file-workers"
	Exec_group  = "exec-workers"
)

func SetUpRedis(t *testing.T) *broker.RedisStore {
	ctx := context.Background()
	redisC, err := testcontainers.Run(ctx, "redis:latest",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
			wait.ForLog("Ready to accept connections"),
		),
	)

	testcontainers.CleanupContainer(t, redisC)
	require.NoError(t, err)

	host, err := redisC.Host(ctx)
	if err != nil {
		t.Error(err)
	}

	port, err := redisC.MappedPort(ctx, "6379/tcp")

	endpoint := fmt.Sprintf("redis://%s:%s", host, port.Port())

	client, err := broker.Redis_Connect(endpoint)
	if err != nil {
		t.Error(err)
	}
	streamsAndGroups := map[string]string{
		Email_stream: Email_group,
		File_stream:  File_group,
		Exec_stream:  Exec_group,
	}

	for stream, group := range streamsAndGroups {
		if err = broker.InitConsumerGroupsAndStreams(context.Background(), stream, group, client.Conn); err != nil {
		}
		continue
	}

	return client
}
