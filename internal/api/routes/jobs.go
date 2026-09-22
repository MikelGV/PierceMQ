package routes

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/google/uuid"
)

// NewJobsHandler serves GET /v1/jobs/{id} and GET /v1/jobs/{id}/events.
// Registered on the "/v1/jobs/" prefix; methods other than GET -> 405.
func NewJobsHandler(store *jobs.JobsStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if _, ok := UserIDFromContext(r.Context()); !ok {
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
		if sub == "events" {
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
			return
		}
		if sub != "" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		job, err := store.GetJobByID(r.Context(), jobID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get job failed"})
			return
		}
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
		writeJSON(w, http.StatusOK, resp)
	}
}
