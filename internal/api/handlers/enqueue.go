package handlers

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
)

var allowedTypes = map[string]bool{
	"email": true, "file": true, "file_processing": true,
	"exec": true, "exec_processing": true,
}

type enqueueRequest struct {
	Type           string         `json:"type"`
	QueueName      string         `json:"queue_name"`
	Payload        map[string]any `json:"payload"`
	Priority       int16          `json:"priority"`
	MaxRetry       int16          `json:"max_retry"`
	IdempotencyKey string         `json:"idempotency_key"`
	ScheduledAt    string         `json:"scheduled_at"`
}

// NewEnqueueHandler implements the DB-first dual write: PG insert first,
// then XADD of the job ref. On XADD failure the PG row stays pending and the
// stale-pending sweeper re-XADDs it — never roll back after commit.
// Future-scheduled jobs skip XADD; the scheduler service dispatches them.
func NewEnqueueHandler(store *jobs.JobsStore, rds *broker.RedisStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if _, ok := auth.UserIDFromContext(r.Context()); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		var req enqueueRequest
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
			return
		}
		req.Type = strings.TrimSpace(req.Type)
		req.QueueName = strings.TrimSpace(req.QueueName)
		if !allowedTypes[req.Type] {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type must be one of email, file, exec"})
			return
		}
		if req.QueueName == "" {
			req.QueueName = req.Type + "-high"
		}
		if req.Payload == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload is required"})
			return
		}

		in := task.Job{
			Type:      req.Type,
			QueueName: req.QueueName,
			Payload:   req.Payload,
			Priority:  req.Priority,
			MaxRetry:  req.MaxRetry,
			Status:    task.JobPending,
		}
		if req.IdempotencyKey != "" {
			in.IdempotencyKey = sql.NullString{String: req.IdempotencyKey, Valid: true}
		}
		if req.ScheduledAt != "" {
			ts, err := time.Parse(time.RFC3339, req.ScheduledAt)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "scheduled_at must be RFC3339"})
				return
			}
			in.ScheduledAt = sql.NullTime{Time: ts, Valid: true}
			if ts.After(time.Now().Add(time.Minute)) {
				in.Status = task.JobScheduled
			}
		}

		created, err := store.CreateJob(r.Context(), in)
		if err != nil {
			if errors.Is(err, jobs.ErrJobExists) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate idempotency key"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "create job failed"})
			return
		}

		// Scheduled-future jobs wait for the scheduler; no stream write.
		if created.Status == task.JobScheduled {
			writeJSON(w, http.StatusCreated, map[string]any{
				"job_id": created.JobID.String(), "status": string(created.Status),
			})
			return
		}

		stream, _, err := queue.StreamFor(created.QueueName, created.Priority)
		if err != nil {
			writeJSON(w, http.StatusCreated, map[string]any{
				"job_id": created.JobID.String(), "status": string(created.Status),
				"warning": "job stored but queue is unknown; fix queue_name",
			})
			return
		}
		if rds == nil || rds.Conn == nil {
			writeJSON(w, http.StatusAccepted, map[string]any{
				"job_id": created.JobID.String(), "status": string(created.Status),
				"warning": "queued_in_db_not_dispatched: redis unavailable",
			})
			return
		}
		msgID, err := rds.AddJobRefToStream(r.Context(), stream, created)
		if err != nil {
			writeJSON(w, http.StatusAccepted, map[string]any{
				"job_id": created.JobID.String(), "status": string(created.Status),
				"warning": "queued_in_db_not_dispatched: " + err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"job_id": created.JobID.String(), "status": string(created.Status),
			"stream": stream, "msg_id": msgID,
		})
	}
}
