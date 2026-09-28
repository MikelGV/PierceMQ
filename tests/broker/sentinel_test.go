package broker_test

import (
	"testing"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/stretchr/testify/require"
)

// TestParseSentinelAddrs covers the REDIS_SENTINELS parsing: comma-separated
// host:port list, whitespace tolerated, empty means single-node mode.
func TestParseSentinelAddrs(t *testing.T) {
	require.Empty(t, broker.ParseSentinelAddrs(""))
	require.Empty(t, broker.ParseSentinelAddrs("  "))

	addrs := broker.ParseSentinelAddrs("sentinel-1:26379,sentinel-2:26379")
	require.Equal(t, []string{"sentinel-1:26379", "sentinel-2:26379"}, addrs)

	addrs = broker.ParseSentinelAddrs(" sentinel-1:26379 , sentinel-2:26379 ,,")
	require.Equal(t, []string{"sentinel-1:26379", "sentinel-2:26379"}, addrs)
}

// TestFailoverOptionsPlumbing proves sentinel mode carries the master name,
// sentinel addrs, and the Redis password (parsed from the redis:// URL) into
// the go-redis failover options — without touching the network.
func TestFailoverOptionsPlumbing(t *testing.T) {
	opts, err := broker.FailoverOptionsFor("mymaster",
		[]string{"sentinel-1:26379", "sentinel-2:26379"},
		"redis://:s3cret@redis:6379/0")
	require.NoError(t, err)
	require.Equal(t, "mymaster", opts.MasterName)
	require.Equal(t, []string{"sentinel-1:26379", "sentinel-2:26379"}, opts.SentinelAddrs)
	require.Equal(t, "s3cret", opts.Password)
	require.Equal(t, 0, opts.DB)

	_, err = broker.FailoverOptionsFor("", []string{"s:26379"}, "redis://:p@h:6379/0")
	require.Error(t, err, "empty master must fail")

	_, err = broker.FailoverOptionsFor("m", nil, "redis://:p@h:6379/0")
	require.Error(t, err, "no sentinels must fail")

	_, err = broker.FailoverOptionsFor("m", []string{"s:26379"}, "://bad-url")
	require.Error(t, err, "bad URL must fail")
}
