package worker

import "github.com/MikelGV/PierceMQ/internal/task"

type Worker struct {
	ID         int
	JobChannel chan *task.Job
	Worker     chan *Worker
}

/**
	* Create a new worker pool
**/
func (*Worker) NewWorker(id int, workerpool chan *Worker) *Worker {
	return &Worker{
		ID:         id,
		JobChannel: make(chan *task.Job),
		Worker:     workerpool,
	}
}

/**
	* Runs the worker main loop
**/
func (*Worker) Run() error {
	return nil
}
