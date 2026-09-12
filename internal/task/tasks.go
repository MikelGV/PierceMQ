package task

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type TaskRequest struct {
	Id      int
	Type    string
	Payload map[string]any
	Attempt int
}

// JobStatus mirrors the job_status enum (migrations/000002).
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobQueued    JobStatus = "queued"
	JobScheduled JobStatus = "scheduled"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
)

// Job is the canonical job model, mirroring the jobs table
// (migrations/000003). DB-mapped fields use sql.Null* for nullable
// columns; Payload/MsgID are runtime-only (Redis wire, never in SQL).
type Job struct {
	JobID          uuid.UUID
	MsgID          string // Redis stream entry ID, runtime only
	Status         JobStatus
	Type           string
	Payload        any // runtime payload, never written to SQL directly
	PayloadRef     sql.NullString
	QueueName      string
	Priority       int16
	AttemptCount   int16
	MaxRetry       int16
	WorkerID       sql.NullString
	ClaimToken     uuid.NullUUID
	IdempotencyKey sql.NullString
	ScheduledAt    sql.NullTime
	CreatedAt      time.Time
	StartedAt      sql.NullTime
	CompletedAt    sql.NullTime
	HeartbeatAt    sql.NullTime
	LastError      sql.NullString
}

/**
* To fields has to be changed once i write the custom encode decode i think
**/
// Marshals payload
func (t TaskRequest) ToFields() map[string]any {
	payloadBytes, err := json.Marshal(t.Payload)
	if err != nil {
		return nil
	}

	return map[string]any{
		"type":    t.Type,
		"payload": string(payloadBytes),
		"attempt": t.Attempt,
	}
}

// Unmarshal from Redis
func FromFields(fields map[string]any) (*TaskRequest, error) {
	taskType, ok := fields["type"].(string)

	if !ok {
		return nil, fmt.Errorf("missing task type")
	}

	payloadStr, ok := fields["payload"].(string)
	if !ok {
		return nil, fmt.Errorf("missing task payload")
	}

	var payload map[string]any

	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return nil, err
	}

	attempt := 0
	if a, ok := fields["attempt"]; ok {
		switch v := a.(type) {
		case string:
			attempt, _ = strconv.Atoi(v)
		case int64:
			attempt = int(v)
		case float64:
			attempt = int(v)
		}
	}

	return &TaskRequest{
		Type:    taskType,
		Payload: payload,
		Attempt: attempt,
	}, nil
}

// ToJobFields serializes a DB-first job ref for XADD. The stream carries
// job_id, job_type, status, and delivery metadata (queue, attempt,
// max_retry, priority, idempotency_key, timestamps, payload) until the job
// is completed + acked or expired. All values are strings for Redis.
func (j Job) ToJobFields() map[string]any {
	m := map[string]any{
		"job_id":     j.JobID.String(),
		"job_type":   j.Type,
		"status":     string(j.Status),
		"queue_name": j.QueueName,
		"attempt":    strconv.Itoa(int(j.AttemptCount)),
		"max_retry":  strconv.Itoa(int(j.MaxRetry)),
		"priority":   strconv.Itoa(int(j.Priority)),
		"created_at": j.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if j.IdempotencyKey.Valid {
		m["idempotency_key"] = j.IdempotencyKey.String
	}
	if j.ScheduledAt.Valid {
		m["scheduled_at"] = j.ScheduledAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if j.PayloadRef.Valid && j.PayloadRef.String != "" {
		m["payload"] = j.PayloadRef.String
	} else if j.Payload != nil {
		if b, err := json.Marshal(j.Payload); err == nil {
			m["payload"] = string(b)
		}
	}
	return m
}

// JobFromFields parses a DB-first job ref from stream values. Requires
// job_id + job_type; everything else is best-effort with zero values on
// absence. Returns an error only when identity fields are missing or
// malformed — callers fall back to legacy FromFields then.
func JobFromFields(fields map[string]any) (*Job, error) {
	rawID, ok := fields["job_id"].(string)
	if !ok || rawID == "" {
		return nil, fmt.Errorf("missing job_id")
	}
	id, err := uuid.Parse(rawID)
	if err != nil {
		return nil, fmt.Errorf("invalid job_id: %w", err)
	}
	rawType, ok := fields["job_type"].(string)
	if !ok || rawType == "" {
		// Back-compat: some producers may use "type".
		rawType, ok = fields["type"].(string)
		if !ok || rawType == "" {
			return nil, fmt.Errorf("missing job_type")
		}
	}

	job := &Job{JobID: id, Type: rawType}
	if s, ok := fields["status"].(string); ok {
		job.Status = JobStatus(s)
	}
	if s, ok := fields["queue_name"].(string); ok {
		job.QueueName = s
	}
	job.AttemptCount = int16(parseNum(fields["attempt"]))
	job.MaxRetry = int16(parseNum(fields["max_retry"]))
	job.Priority = int16(parseNum(fields["priority"]))
	if s, ok := fields["idempotency_key"].(string); ok && s != "" {
		job.IdempotencyKey = sql.NullString{String: s, Valid: true}
	}
	if s, ok := fields["created_at"].(string); ok {
		if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
			job.CreatedAt = ts
		}
	}
	if s, ok := fields["scheduled_at"].(string); ok && s != "" {
		if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
			job.ScheduledAt = sql.NullTime{Time: ts, Valid: true}
		}
	}
	if s, ok := fields["payload"].(string); ok && s != "" {
		job.PayloadRef = sql.NullString{String: s, Valid: true}
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			job.Payload = m
		} else {
			job.Payload = s
		}
	}
	return job, nil
}

func parseNum(v any) int {
	switch n := v.(type) {
	case string:
		i, _ := strconv.Atoi(n)
		return i
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}
