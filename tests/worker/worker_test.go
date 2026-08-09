package worker

import (
	"testing"

	"github.com/MikelGV/PierceMQ/internal/worker"
)

/**
* I need to rewrite this tests to because I might change things in the create
* function
**/
func TestNewWorker(t *testing.T) {
	t.Run("Worker is created successfully", func(t *testing.T) {
		var w *worker.Worker

		// I have to think for a way to increment the workers or increament the active pools
		woker := w.NewWorker(1, make(chan *worker.Worker))

		t.Log(woker)
	})

	t.Skip("Worker creation fails due to bad/missing connection", func(t *testing.T) {})
}

func TestRun(t *testing.T) {
	t.Skip("Worker pool is created successfully", func(t *testing.T) {})

	t.Skip("Worker creation fails due to bad/missing connection", func(t *testing.T) {})
}
