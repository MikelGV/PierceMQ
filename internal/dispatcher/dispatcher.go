package dispatcher

import (
	"context"
	"sync"

	"github.com/MikelGV/PierceMQ/internal/task"
)

type Dispatcher struct {
	WorkerPool chan chan *task.Job
	Jobqueue   chan *task.Job

	handlers map[string]HandlerFunc
	mu       sync.RWMutex
}

func (d *Dispatcher) NewDispatcher(workerCount int) (*Dispatcher, error) {
	return &Dispatcher{
		WorkerPool: make(chan chan *task.Job, workerCount),
		Jobqueue:   make(chan *task.Job, 100),
		handlers:   make(map[string]HandlerFunc),
	}, nil
}

func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case job := <-d.Jobqueue:
			// Blocking handoff: no per-job goroutine. When all workers
			// are busy this blocks, Jobqueue fills (cap 100), and
			// DispatchTask blocks the ServeJobs poll loop — natural
			// backpressure instead of unbounded goroutine fan-out.
			select {
			case jobCh := <-d.WorkerPool:
				select {
				case jobCh <- job:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
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
