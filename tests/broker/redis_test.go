package broker_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRedisConnection(t *testing.T) {
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

	/**
	endpoint, err := redisC.Endpoint(ctx, "")
	if err != nil {
		t.Error(err)
	}
	**/

	client, err := broker.Redis_Connect(endpoint)
	if err != nil {
		t.Error(err)
	}

	t.Logf("It worked! %+v", client)
}
