package worker

import "github.com/MikelGV/PierceMQ/internal/task"

type Worker struct {
	ID         int
	JobChannel chan *task.Job
	Worker     chan *Worker
}

func NewWorker() {
}
