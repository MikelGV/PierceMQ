package handlers

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

// jobResponse renders one job in the §9 API shape, shared by single-get and
// list. Timestamps are RFC3339; absent nullable fields are omitted.
func jobResponse(job task.Job) map[string]any {
	resp := map[string]any{
		"job_id": job.JobID.String(), "status": string(job.Status),
		"type": job.Type, "queue_name": job.QueueName,
		"priority": job.Priority, "attempt_count": job.AttemptCount,
		"max_retry": job.MaxRetry, "created_at": job.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
	if job.Payload != nil {
		resp["payload"] = job.Payload
	} else if job.PayloadRef.Valid {
		resp["payload"] = job.PayloadRef.String
	}
	if job.IdempotencyKey.Valid {
		resp["idempotency_key"] = job.IdempotencyKey.String
	}
	if job.ScheduledAt.Valid {
		resp["scheduled_at"] = job.ScheduledAt.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if job.StartedAt.Valid {
		resp["started_at"] = job.StartedAt.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if job.CompletedAt.Valid {
		resp["completed_at"] = job.CompletedAt.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if job.LastError.Valid {
		resp["last_error"] = job.LastError.String
	}
	return resp
}

// NewJobsHandler serves the /v1/jobs/{id} subtree:
//
//	GET    /v1/jobs/{id}          single job (§9.2)
//	GET    /v1/jobs/{id}/events   status timeline
//	DELETE /v1/jobs/{id}          cancel a not-yet-running job (§9.3)
//	POST   /v1/jobs/{id}/retry    replay a failed job (§9.6)
func NewJobsHandler(store *jobs.JobsStore, rds *broker.RedisStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.UserIDFromContext(r.Context()); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		if rest == "" || strings.Contains(rest, "/../") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing job id"})
			return
		}
		idPart, sub, _ := strings.Cut(rest, "/")
		jobID, err := uuid.Parse(idPart)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid job id"})
			return
		}
		switch {
		case sub == "" && r.Method == http.MethodGet:
			getSingle(w, r, store, jobID)
		case sub == "" && r.Method == http.MethodDelete:
			cancelJob(w, r, store, jobID)
		case sub == "events" && r.Method == http.MethodGet:
			getEvents(w, r, store, jobID)
		case sub == "retry" && r.Method == http.MethodPost:
			retryJob(w, r, store, rds, jobID)
		case sub == "" || sub == "events" || sub == "retry":
			w.Header().Set("Allow", allowedFor(sub))
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		}
	}
}

func allowedFor(sub string) string {
	switch sub {
	case "":
		return "GET, DELETE"
	case "events":
		return http.MethodGet
	case "retry":
		return http.MethodPost
	default:
		return ""
	}
}

func getSingle(w http.ResponseWriter, r *http.Request, store *jobs.JobsStore, jobID uuid.UUID) {
	userID, _ := auth.UserIDFromContext(r.Context())
	job, err := store.GetJobByID(r.Context(), jobID, userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get job failed"})
		return
	}
	writeJSON(w, http.StatusOK, jobResponse(job))
}

func getEvents(w http.ResponseWriter, r *http.Request, store *jobs.JobsStore, jobID uuid.UUID) {
	userID, _ := auth.UserIDFromContext(r.Context())
	// Ownership gate: events have no owner column, so the parent job guards them.
	if _, err := store.GetJobByID(r.Context(), jobID, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get job failed"})
		return
	}
	events, err := store.GetJobEvents(r.Context(), jobID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "list events failed"})
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, e := range events {
		m := map[string]any{
			"event_id": e.EventID.String(),
			"new_status": string(e.NewStatus), "occurred_at": e.Occurred.UTC().Format("2006-01-02T15:04:05Z07:00"),
		}
		if e.OldStatus.Valid {
			m["old_status"] = e.OldStatus.String
		}
		if e.WorkerID.Valid {
			m["worker_id"] = e.WorkerID.String
		}
		if e.Reason.Valid {
			m["reason"] = e.Reason.String
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": jobID.String(), "events": out})
}

// cancelJob implements DELETE /v1/jobs/{id} (§9.3): only not-yet-running jobs
// may cancel; anything else is a 409. The orphaned stream entry (if any) is
// left for the claim guard to ack + skip.
func cancelJob(w http.ResponseWriter, r *http.Request, store *jobs.JobsStore, jobID uuid.UUID) {
	userID, _ := auth.UserIDFromContext(r.Context())
	if _, err := store.GetJobByID(r.Context(), jobID, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get job failed"})
		return
	}
	out, err := store.CancelJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, jobs.ErrNotCancellable) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "job cannot be cancelled in its current status"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "cancel job failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": out.JobID.String(), "status": string(out.Status),
	})
}

// retryJob implements POST /v1/jobs/{id}/retry (§9.6): a failed job returns
// to pending and is re-dispatched to its stream. On dispatch failure the row
// stays pending (202 + warning) for the reaper sweep — never rolled back.
func retryJob(w http.ResponseWriter, r *http.Request, store *jobs.JobsStore, rds *broker.RedisStore, jobID uuid.UUID) {
	userID, _ := auth.UserIDFromContext(r.Context())
	if _, err := store.GetJobByID(r.Context(), jobID, userID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get job failed"})
		return
	}
	out, err := store.RetryJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, jobs.ErrNotRetryable) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "only failed jobs can be retried"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "retry job failed"})
		return
	}

	stream, _, err := queue.StreamFor(out.QueueName, out.Priority)
	if err != nil {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"job_id": out.JobID.String(), "status": string(out.Status),
			"warning": "job retried but queue is unknown; fix queue_name",
		})
		return
	}
	if rds == nil || rds.Conn == nil {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"job_id": out.JobID.String(), "status": string(out.Status),
			"warning": "retried_in_db_not_dispatched: redis unavailable",
		})
		return
	}
	msgID, err := rds.AddJobRefToStream(r.Context(), stream, out)
	if err != nil {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"job_id": out.JobID.String(), "status": string(out.Status),
			"warning": "retried_in_db_not_dispatched: " + err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": out.JobID.String(), "status": string(out.Status),
		"stream": stream, "msg_id": msgID,
	})
}
