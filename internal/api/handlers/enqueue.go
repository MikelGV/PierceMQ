package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/cronx"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/google/uuid"
)

var allowedTypes = map[string]bool{
	"email": true, "file": true, "file_processing": true,
	"exec": true, "exec_processing": true,
}

// MaxPayloadBytes caps job payload size per §3.2 (15 KB).
const MaxPayloadBytes = 15 * 1024

type enqueueRequest struct {
	Type           string         `json:"type"`
	QueueName      string         `json:"queue_name"`
	Payload        map[string]any `json:"payload"`
	Priority       int16          `json:"priority"`
	MaxRetry       int16          `json:"max_retry"`
	IdempotencyKey string         `json:"idempotency_key"`
	ScheduledAt    string         `json:"scheduled_at"`
	// Cron starts a recurring series (5-field standard cron or @-descriptor);
	// CronTZ is the IANA timezone the expression is evaluated in (UTC).
	Cron   string `json:"cron"`
	CronTZ string `json:"cron_tz"`
}

// MaxEnqueueBodyBytes caps the whole enqueue envelope (not just payload):
// non-payload keys bypass the 15KB payload check, so the envelope needs its
// own bound. 256KB admits any legal job with wide headroom.
const MaxEnqueueBodyBytes = 256 * 1024

// decodeEnqueueRequest tolerantly decodes the body: unknown fields (e.g.
// retry_policy/backoff from §9.1 clients) are ignored, schedule_at is
// accepted as an alias of scheduled_at, and retry_policy.max_retries fills
// max_retry when the flat field is absent. The envelope is capped at
// MaxEnqueueBodyBytes (errBodyTooLarge on overflow).
func decodeEnqueueRequest(w http.ResponseWriter, r *http.Request, req *enqueueRequest) error {
	if r.ContentLength > MaxEnqueueBodyBytes {
		return errBodyTooLarge
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxEnqueueBodyBytes))
	if err := dec.Decode(&raw); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return errBodyTooLarge
		}
		return err
	}
	get := func(keys ...string) json.RawMessage {
		for _, k := range keys {
			if v, ok := raw[k]; ok {
				return v
			}
		}
		return nil
	}
	if v := get("type"); v != nil {
		if err := json.Unmarshal(v, &req.Type); err != nil {
			return err
		}
	}
	if v := get("queue_name"); v != nil {
		if err := json.Unmarshal(v, &req.QueueName); err != nil {
			return err
		}
	}
	if v := get("payload"); v != nil {
		if err := json.Unmarshal(v, &req.Payload); err != nil {
			return err
		}
	}
	if v := get("priority"); v != nil {
		if err := json.Unmarshal(v, &req.Priority); err != nil {
			return err
		}
	}
	if v := get("max_retry"); v != nil {
		if err := json.Unmarshal(v, &req.MaxRetry); err != nil {
			return err
		}
	} else if v := get("retry_policy"); v != nil {
		var rp struct {
			MaxRetries *int16 `json:"max_retries"`
		}
		if err := json.Unmarshal(v, &rp); err == nil && rp.MaxRetries != nil {
			req.MaxRetry = *rp.MaxRetries
		}
	}
	if v := get("idempotency_key"); v != nil {
		if err := json.Unmarshal(v, &req.IdempotencyKey); err != nil {
			return err
		}
	}
	if v := get("scheduled_at", "schedule_at"); v != nil {
		if err := json.Unmarshal(v, &req.ScheduledAt); err != nil {
			return err
		}
	}
	if v := get("cron", "cron_expr"); v != nil {
		if err := json.Unmarshal(v, &req.Cron); err != nil {
			return err
		}
	}
	if v := get("cron_tz", "timezone"); v != nil {
		if err := json.Unmarshal(v, &req.CronTZ); err != nil {
			return err
		}
	}
	return nil
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
		userID, _ := auth.UserIDFromContext(r.Context())
		var req enqueueRequest
		if err := decodeEnqueueRequest(w, r, &req); err != nil {
			writeDecodeError(w, err)
			return
		}
		// Idempotency-Key header (§9.1) wins; body field is the fallback.
		if h := strings.TrimSpace(r.Header.Get("Idempotency-Key")); h != "" {
			req.IdempotencyKey = h
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
		if b, err := json.Marshal(req.Payload); err != nil || len(b) > MaxPayloadBytes {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload exceeds 15KB"})
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
		in.OwnerUserID = uuid.NullUUID{UUID: userID, Valid: true}
		if req.IdempotencyKey != "" {
			in.IdempotencyKey = sql.NullString{String: req.IdempotencyKey, Valid: true}
		}
		if strings.TrimSpace(req.Cron) != "" {
			// Recurring series: cron and one-shot scheduled_at are mutually
			// exclusive; the first occurrence fires at the next cron time.
			if strings.TrimSpace(req.ScheduledAt) != "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cron and scheduled_at are mutually exclusive"})
				return
			}
			tz := strings.TrimSpace(req.CronTZ)
			if tz == "" {
				tz = cronx.DefaultTZ
			}
			next, err := cronx.NextAfter(req.Cron, tz, time.Now().UTC())
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cron: " + err.Error()})
				return
			}
			seriesID := uuid.New()
			in.IsRecurring = true
			in.CronExpr = sql.NullString{String: strings.TrimSpace(req.Cron), Valid: true}
			in.CronTZ = sql.NullString{String: tz, Valid: true}
			in.SeriesID = uuid.NullUUID{UUID: seriesID, Valid: true}
			in.ScheduledAt = sql.NullTime{Time: next.UTC(), Valid: true}
			in.Status = task.JobScheduled
		} else if req.ScheduledAt != "" {
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
			resp := map[string]any{
				"job_id": created.JobID.String(), "status": string(created.Status),
			}
			if created.IsRecurring {
				resp["series_id"] = created.SeriesID.UUID.String()
				resp["cron"] = created.CronExpr.String
				resp["cron_tz"] = created.CronTZ.String
				if created.ScheduledAt.Valid {
					resp["scheduled_at"] = created.ScheduledAt.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
				}
			}
			writeJSON(w, http.StatusCreated, resp)
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
