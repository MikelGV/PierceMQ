package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startRedisNode runs one redis:alpine3.22 node with the given server args.
// It returns the container and a host-reachable client for it.
func startRedisNode(t *testing.T, ctx context.Context, args ...string) (testcontainers.Container, *redis.Client) {
	t.Helper()
	c, err := testcontainers.Run(ctx, "redis:alpine3.22",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithCmd(args...),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
			wait.ForLog("Ready to accept connections"),
		),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port.Port()),
		Password: "testpw",
	})
	t.Cleanup(func() { _ = client.Close() })
	return c, client
}

func containerIP(t *testing.T, ctx context.Context, c testcontainers.Container) string {
	t.Helper()
	inspect, err := c.Inspect(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, inspect.NetworkSettings.IPAddress)
	return inspect.NetworkSettings.IPAddress
}

func roleOf(t *testing.T, ctx context.Context, client *redis.Client) string {
	t.Helper()
	role, err := client.Do(ctx, "role").Result()
	require.NoError(t, err)
	parts, ok := role.([]any)
	require.True(t, ok && len(parts) > 0, "unexpected ROLE reply: %v", role)
	name, ok := parts[0].(string)
	require.True(t, ok, "unexpected ROLE reply: %v", role)
	return name
}

// TestSentinelPromotesReplicaOnPrimaryLoss is the §12.6 Redis-failover
// recovery test against a primary + replica + sentinel tier mirroring
// docker-compose.yaml: killing the primary must promote the replica (quorum
// 1 here for speed; compose uses 3 sentinels / quorum 2), the promoted node
// must accept writes, and pre-failover data replicated before the kill must
// survive. Async-replication loss of the last in-flight ms is accepted by
// design (§11.3.2); PostgreSQL-backed re-enqueue covers it.
func TestSentinelPromotesReplicaOnPrimaryLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	primary, primaryClient := startRedisNode(t, ctx,
		"redis-server", "--requirepass", "testpw", "--port", "6379")
	primaryIP := containerIP(t, ctx, primary)

	_, replicaClient := startRedisNode(t, ctx,
		"redis-server", "--requirepass", "testpw", "--masterauth", "testpw",
		"--replicaof", primaryIP, "6379", "--port", "6379")

	conf := fmt.Sprintf("port 26379\\ndir /tmp\\n"+
		"sentinel monitor mymaster %s 6379 1\\n"+
		"sentinel auth-pass mymaster testpw\\n"+
		"sentinel down-after-milliseconds mymaster 2000\\n"+
		"sentinel failover-timeout mymaster 10000\\n", primaryIP)
	sentinel, err := testcontainers.Run(ctx, "redis:alpine3.22",
		testcontainers.WithExposedPorts("26379/tcp"),
		testcontainers.WithEntrypoint("sh", "-c",
			fmt.Sprintf("printf '%s' > /tmp/sentinel.conf && redis-server /tmp/sentinel.conf --sentinel", conf)),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("26379/tcp"),
			wait.ForLog("+monitor master mymaster"),
		),
	)
	testcontainers.CleanupContainer(t, sentinel)
	require.NoError(t, err)

	sentinelHost, err := sentinel.Host(ctx)
	require.NoError(t, err)
	sentinelPort, err := sentinel.MappedPort(ctx, "26379/tcp")
	require.NoError(t, err)
	sentinelClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", sentinelHost, sentinelPort.Port()),
	})
	t.Cleanup(func() { _ = sentinelClient.Close() })

	// Precondition: a key written on the primary replicates before the kill.
	require.NoError(t, primaryClient.Set(ctx, "pre-failover", "v", 0).Err())
	require.Eventually(t, func() bool {
		v, err := replicaClient.Get(ctx, "pre-failover").Result()
		return err == nil && v == "v"
	}, 30*time.Second, 500*time.Millisecond, "replica must sync before the kill")
	require.Equal(t, "slave", roleOf(t, ctx, replicaClient))

	// Kill the primary. Sentinel must promote the replica.
	tenSec := 10 * time.Second
	require.NoError(t, primary.Stop(ctx, &tenSec))

	require.Eventually(t, func() bool {
		return roleOf(t, ctx, replicaClient) == "master"
	}, 90*time.Second, time.Second, "sentinel must promote the replica")

	masterAddr, err := sentinelClient.Do(ctx, "sentinel", "get-master-addr-by-name", "mymaster").Result()
	require.NoError(t, err)
	t.Logf("sentinel reports master: %v", masterAddr)

	// Promoted node accepts writes; replicated data survived.
	require.NoError(t, replicaClient.Set(ctx, "post-failover", "w", 0).Err())
	v, err := replicaClient.Get(ctx, "pre-failover").Result()
	require.NoError(t, err)
	require.Equal(t, "v", v)
}
