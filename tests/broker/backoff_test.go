package broker_test

import (
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/stretchr/testify/require"
)

// TestReadBackoffProgression covers the ServeJobs reconnect backoff: it
// starts at 100ms, doubles per consecutive failure, and caps at 5s so a dead
// Redis never busy-spins the worker nor sleeps past recovery for long.
func TestReadBackoffProgression(t *testing.T) {
	require.Equal(t, 100*time.Millisecond, broker.NextReadBackoff(0))
	require.Equal(t, 200*time.Millisecond, broker.NextReadBackoff(100*time.Millisecond))
	require.Equal(t, 400*time.Millisecond, broker.NextReadBackoff(200*time.Millisecond))
	require.Equal(t, 5*time.Second, broker.NextReadBackoff(4*time.Second))
	require.Equal(t, 5*time.Second, broker.NextReadBackoff(5*time.Second))
	require.Equal(t, 5*time.Second, broker.NextReadBackoff(time.Hour))
}
