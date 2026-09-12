package broker

import (
	"context"

	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// JobStore is the worker-side subset of JobsStore needed by the consume
// path (claim → heartbeat → complete/fail). Defined here so the broker
// stays decoupled from storage and tests can run Redis-only with nil.
type JobStore interface {
	ClaimRunning(ctx context.Context, jobID uuid.UUID, workerID string) (task.Job, uuid.UUID, error)
	Heartbeat(ctx context.Context, jobID, claimToken uuid.UUID) error
	Complete(ctx context.Context, jobID, claimToken uuid.UUID) error
	FailOrRetry(ctx context.Context, jobID, claimToken uuid.UUID, lastErr string) (task.Job, error)
}
