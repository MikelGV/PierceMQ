package broker_test

import (
	"context"
	"testing"

	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
)

func TestRedisConnection(t *testing.T) {
	store := utils_test.SetUpRedis(t)

	err := store.Conn.Ping(context.Background()).Err()
	require.NoError(t, err)
}
