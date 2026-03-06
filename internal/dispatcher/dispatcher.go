package dispatcher

import (
	"context"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/MikelGV/PierceMQ/internal/worker"
)

type Dispatcher struct {
	WorkerPool chan *worker.Worker
	Jobqueue   chan *task.Job
}

func (d *Dispatcher) NewDispatcher(workerCount int) (*Dispatcher, error) {
	return &Dispatcher{
		WorkerPool: make(chan *worker.Worker, workerCount),
		Jobqueue:   make(chan *task.Job, 100),
	}, nil
}

func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case job := <-d.Jobqueue:
			go func(job *task.Job) {

				worker := <-d.WorkerPool
				worker.JobChannel <- job
			}(job)
		case <-ctx.Done():
			return
		}
	}
}

// Here we decide what to do with the task, we assign the job into an available worker

func (d *Dispatcher) DispatchTask(ctx context.Context, job *task.Job) error {
	select {
	case d.Jobqueue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
