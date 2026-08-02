package worker

import (
	"testing"

	"github.com/MikelGV/PierceMQ/internal/worker"
)

func TestNewWorker(t *testing.T) {
	t.Run("Worker is created successfully", func(t *testing.T) {
		var w *worker.Worker

		woker := w.NewWorker(1, make(chan *worker.Worker))

		t.Log(woker)
	})

	t.Skip("Worker creation fails due to bad/missing connection", func(t *testing.T) {})
}

func TestRun_Pool(t *testing.T) {
	t.Skip("Worker pool is created successfully", func(t *testing.T) {})

	t.Skip("Worker creation fails due to bad/missing connection", func(t *testing.T) {})
}
