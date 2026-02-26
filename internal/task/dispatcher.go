package task

import "context"

/**
* I think here i design the job structure but i haven't thought if i have to save
* it in the db once the task is already saved in the db
**/
type Job struct {
	ID int
}

/**
* I need to think what the we do when the http handler gets the task how we do
* things, how we end up with it in the worker and things like that.
**/

type Dispatcher struct {
	//Workerpool chan *Worker
	Jobqueue chan *Job
}

func (d *Dispatcher) NewDispatcher(ctx context.Context, tsk *TaskRequest) {}

// Here we decide what to do with the task, we assign the job into an available worker
func DispatchTask() {}
