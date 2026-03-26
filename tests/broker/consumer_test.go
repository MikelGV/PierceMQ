package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/require"
)

func TestConsumeJobs(t *testing.T) {
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)

	defer cancel()

	store := utils_test.SetUpRedis(t)

	var streamKey = "tasks:email"
	var groupKey = "email-workers"
	var consumerKey = "idk"

	err := store.ConsumeJobs(ctx, streamKey, groupKey, consumerKey, &dispatcher.Dispatcher{})
	require.NoError(t, err, "this should work!")

}
