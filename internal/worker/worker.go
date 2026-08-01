package worker

import "github.com/MikelGV/PierceMQ/internal/task"

type Worker struct {
	ID         int
	JobChannel chan *task.Job
	Worker     chan *Worker
}

/**
	* Create a new worker
**/
func (*Worker) NewWorker() (*Worker, error) {
	return nil, nil
}

/**
	* Run every worker that has been created and create a pool of them
**/
func (*Worker) Run_Pool() ([]*Worker, error) {
	return nil, nil
}
