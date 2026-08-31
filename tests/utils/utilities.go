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
		Email_low_stream:  Email_group_low,
		File_low_stream:   File_group_low,
		Exec_low_stream:   Exec_group_low,
		Email_high_stream: Email_group_high,
		File_high_stream:  File_group_high,
		Exec_high_stream:  Exec_group_high,
	}

	for stream, group := range streamsAndGroups {
		if err = broker.InitConsumerGroupsAndStreams(context.Background(), stream, group, client.Conn); err != nil {
		}
		continue
	}

	return client
}
