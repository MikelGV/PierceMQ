package dispatcher

import (
	"context"

	"github.com/MikelGV/PierceMQ/internal/task"
)

type HandlerFunc func(ctx context.Context, job *task.Job) error

func (d *Dispatcher) Register(tskType string, h HandlerFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[tskType] = h
}
