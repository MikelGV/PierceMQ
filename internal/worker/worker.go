package worker

import (
	"context"
	"errors"
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
	* Here we run the claimjopbs, processJobs the error handling, etc, ...
	* Also here we handle sending and reciving the heartbeats
**/
func (w *Worker) Run(ctx context.Context) error {
	return nil
}

/**
	* This function will claim the jobs and then send them to the
	* ProcessJobs function
**/
func (w *Worker) ClaimJob(wId, job_type, job_payload string, ctx context.Context) error {
	return nil
}

/**
* This function process all jobs differentiating each type of job and doing what
* it requires to complete them
*
 */
func (w *Worker) ProcessJobs(job_type, job_payload string, ctx context.Context) (string, error) {
	if job_type == "email" {
		/**
		* Here we process the job if it's an email type
		**/
	} else if job_type == "exec" {
	} else if job_type == "file" {
	} else {
		return "", errors.New("Job type doesn't match with the allowed ones")
	}

	return "", nil

}
