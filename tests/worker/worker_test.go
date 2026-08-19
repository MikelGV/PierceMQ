package worker_test

import (
	"context"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/MikelGV/PierceMQ/internal/worker"
	utils_test "github.com/MikelGV/PierceMQ/tests/utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWorker(t *testing.T) {
	t.Run("Worker is created successfully", func(t *testing.T) {
		store := utils_test.SetUpRedis(t)
		pool := make(chan *worker.Worker, 10)

		w, err := (&worker.Worker{}).NewWorker(1, pool, store.Conn)

		require.NoError(t, err)
		require.NotNil(t, w)
		assert.Equal(t, 1, w.ID)
		assert.NotNil(t, w.JobChannel)
		assert.Equal(t, 0, cap(w.JobChannel), "job channel should be unbuffered")
		assert.Equal(t, pool, w.Worker)

		pool <- w
		assert.Same(t, w, <-pool)
	})

	t.Run("Worker creation fails due to lost connection", func(t *testing.T) {
		conn := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})

		w, err := (&worker.Worker{}).NewWorker(2, make(chan *worker.Worker, 10), conn)

		require.Error(t, err)
		assert.Nil(t, w)
	})

	t.Run("Worker creation fails with nil connection", func(t *testing.T) {
		w, err := (&worker.Worker{}).NewWorker(3, make(chan *worker.Worker, 10), nil)

		require.Error(t, err)
		assert.Nil(t, w)
	})
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	t.Run("All Workers run, each pick a job from the stream and process the jobs successfully", func(t *testing.T) {
		w := &worker.Worker{
			ID:         1,
			JobChannel: make(chan *task.Job),
			Worker:     make(chan *worker.Worker),
		}

		err := w.Run(ctx)

		assert.NoError(t, err)

	})

	t.Skip("All Workers run, but fail during the job processing", func(t *testing.T) {
		w := &worker.Worker{
			ID:         1,
			JobChannel: make(chan *task.Job),
			Worker:     make(chan *worker.Worker),
		}

		err := w.Run(ctx)

		assert.NoError(t, err)
	})

	t.Skip("All Workers fail", func(t *testing.T) {
		w := &worker.Worker{
			ID:         1,
			JobChannel: make(chan *task.Job),
			Worker:     make(chan *worker.Worker),
		}

		err := w.Run(ctx)

		assert.NoError(t, err)

	})
}

/**
* Here we test what happens when the woker takes a job and calls the ProcessJob
* function to process the task at hand, so the tasks available are: send email,
* file processing, and binary processing
**/
func ProcessJobTest(t *testing.T) {

	t.Run("Email job gets processed correctly", func(t *testing.T) {

	})

	t.Skip("Binary job gets processed correctly", func(t *testing.T) {})

	t.Skip("File processing job gets processed correctly", func(t *testing.T) {})

	t.Run("Email job fails to process due to syntax error", func(t *testing.T) {})

	t.Skip("Binary job fails due to timeout", func(t *testing.T) {})

	t.Skip("File processing job fails due to timeout", func(t *testing.T) {})

	t.Skip("Email job fails due to timeout", func(t *testing.T) {})

}
