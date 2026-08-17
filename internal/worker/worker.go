package worker

import (
	"context"
	"fmt"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	ID         int
	JobChannel chan *task.Job
	Worker     chan *Worker
}

/**
	* Create a new worker pool
**/
func (w *Worker) NewWorker(id int, workerpool chan *Worker, conn *redis.Client) (*Worker, error) {
	if conn == nil {
		return nil, fmt.Errorf("worker %d: nil redis connection", id)
	}

	if err := conn.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("worker %d: lost connection to redis: %w", id, err)
	}

	return &Worker{
		ID:         id,
		JobChannel: make(chan *task.Job),
		Worker:     workerpool,
	}, nil
}

/**
	* Runs the worker main loop
	* Here we run the processJobs the error handling,etc, ...
**/
func (w *Worker) Run(ctx context.Context) error {
	return nil
}

/**
* This function process all jobs differentiating each type of job and doing what
* it requires to complete them
**/
func (w *Worker) processJobs() {

}
