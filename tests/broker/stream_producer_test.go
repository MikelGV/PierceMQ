package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/**
* I need to mock the AddTaskToStream function
**/

func TestAddTaskToStream(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)

	defer cancel()

	store := utils_test.SetUpRedis(t)

	const streamKey = "tasks:email"

	reqs := []*task.TaskRequest{
		{Id: 1, Type: "email", Payload: map[string]any{
			"from": "test@test.com",
			"to":   "test2@test.com",
			"body": "Hello mate this is test how you doing ma boy",
		}},
		{Id: 2, Type: "email", Payload: map[string]any{
			"from": "test2@test.com",
			"to":   "test@test.com",
			"body": "Hello mate this is test2 how you doing ma boy",
		}},
	}

	_, err := store.AddTaskToStream(ctx, streamKey, reqs)
	require.NoError(t, err, "this should work!")

	entries, err := store.Conn.XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	for d, e := range entries {
		t.Logf("%v/n %v/n", d, e)
	}
}
