// Package client is the Go client for PierceMQ: register/login (JWT for
// humans) or a long-lived API key (services), then enqueue + poll jobs.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Client talks to the API. Set Token to a JWT or a pmq_ API key; it is sent
// as Authorization: Bearer on every call.
type Client struct {
	BaseURL    string
	Token      string
	HTTP       *http.Client
	timeFormat string
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	return c.doWithHeaders(ctx, method, path, body, nil, out)
}

func (c *Client) doWithHeaders(ctx context.Context, method, path string, body any, headers map[string]string, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return fmt.Errorf("client: encode: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, &buf)
	if err != nil {
		return fmt.Errorf("client: request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("client: do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("client: %s %s -> %d %v", method, path, resp.StatusCode, errBody)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("client: decode: %w", err)
		}
	}
	return nil
}

// Register creates a user. Password min 8 chars (server-enforced).
func (c *Client) Register(ctx context.Context, name, email, password string) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodPost, "/v1/auth/register",
		map[string]string{"name": name, "email": email, "password": password}, &out)
	return out, err
}

// Login verifies credentials and stores the JWT on the client for chaining.
func (c *Client) Login(ctx context.Context, email, password string) (string, error) {
	var out map[string]string
	if err := c.do(ctx, http.MethodPost, "/v1/auth/login",
		map[string]string{"email": email, "password": password}, &out); err != nil {
		return "", err
	}
	c.Token = out["token"]
	return c.Token, nil
}

// EnqueueRequest mirrors POST /v1/jobs. ScheduledAt is RFC3339 or empty.
// Cron (5-field standard cron or @-descriptor, with CronTZ IANA timezone)
// starts a recurring series instead of a one-shot scheduled_at.
type EnqueueRequest struct {
	Type           string         `json:"type"`
	QueueName      string         `json:"queue_name,omitempty"`
	Payload        map[string]any `json:"payload"`
	Priority       int16          `json:"priority,omitempty"`
	MaxRetry       int16          `json:"max_retry,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	ScheduledAt    string         `json:"scheduled_at,omitempty"`
	Cron           string         `json:"cron,omitempty"`
	CronTZ         string         `json:"cron_tz,omitempty"`
}

// Enqueue performs the DB-first dual write via the API. The idempotency key
// is sent both as Idempotency-Key header (§9.1) and body field.
func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (map[string]any, error) {
	if req.Payload != nil {
		if b, err := json.Marshal(req.Payload); err == nil && len(b) > 15*1024 {
			return nil, fmt.Errorf("client: payload exceeds 15KB (%d bytes)", len(b))
		}
	}
	var out map[string]any
	var headers map[string]string
	if req.IdempotencyKey != "" {
		headers = map[string]string{"Idempotency-Key": req.IdempotencyKey}
	}
	err := c.doWithHeaders(ctx, http.MethodPost, "/v1/jobs", req, headers, &out)
	return out, err
}

// GetJob fetches one job by id.
func (c *Client) GetJob(ctx context.Context, jobID string) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodGet, "/v1/jobs/"+jobID, nil, &out)
	return out, err
}

// GetJobEvents fetches the status timeline for a job.
func (c *Client) GetJobEvents(ctx context.Context, jobID string) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodGet, "/v1/jobs/"+jobID+"/events", nil, &out)
	return out, err
}

// ListJobs lists jobs newest-first, optionally filtered by status
// (scheduled, pending, queued, running, completed, failed, cancelled).
// Use limit <= 0 for the server default (50, max 100).
func (c *Client) ListJobs(ctx context.Context, status string, limit, offset int) (map[string]any, error) {
	path := "/v1/jobs?"
	if status != "" {
		path += "status=" + status + "&"
	}
	if limit > 0 {
		path += fmt.Sprintf("limit=%d&", limit)
	}
	if offset > 0 {
		path += fmt.Sprintf("offset=%d", offset)
	}
	var out map[string]any
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

// Cancel cancels a not-yet-running job (DELETE /v1/jobs/{id}).
// With series=true it cancels every not-yet-running member of the job's
// recurring series instead (?series=true).
func (c *Client) Cancel(ctx context.Context, jobID string) (map[string]any, error) {
	return c.CancelSeries(ctx, jobID, false)
}

// CancelSeries cancels one job, or its whole recurring series when series
// is true.
func (c *Client) CancelSeries(ctx context.Context, jobID string, series bool) (map[string]any, error) {
	path := "/v1/jobs/" + jobID
	if series {
		path += "?series=true"
	}
	var out map[string]any
	err := c.do(ctx, http.MethodDelete, path, nil, &out)
	return out, err
}

// ListSeries lists members of a recurring series newest-first.
func (c *Client) ListSeries(ctx context.Context, seriesID, status string, limit, offset int) (map[string]any, error) {
	path := "/v1/jobs?series_id=" + seriesID + "&"
	if status != "" {
		path += "status=" + status + "&"
	}
	if limit > 0 {
		path += fmt.Sprintf("limit=%d&", limit)
	}
	if offset > 0 {
		path += fmt.Sprintf("offset=%d", offset)
	}
	var out map[string]any
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

// Retry replays a failed job back to pending (POST /v1/jobs/{id}/retry).
func (c *Client) Retry(ctx context.Context, jobID string) (map[string]any, error) {
	return c.RetryWithKey(ctx, jobID, "")
}

// RetryWithKey replays a failed job, sending Idempotency-Key (§9.6) when
// non-empty so resends return the current state instead of re-dispatching.
func (c *Client) RetryWithKey(ctx context.Context, jobID, key string) (map[string]any, error) {
	var out map[string]any
	var headers map[string]string
	if key != "" {
		headers = map[string]string{"Idempotency-Key": key}
	}
	err := c.doWithHeaders(ctx, http.MethodPost, "/v1/jobs/"+jobID+"/retry", nil, headers, &out)
	return out, err
}

// Stats fetches queue and worker statistics (GET /v1/stats).
func (c *Client) Stats(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodGet, "/v1/stats", nil, &out)
	return out, err
}

// CreateAPIKey mints a service key (plaintext returned once).
func (c *Client) CreateAPIKey(ctx context.Context, name string) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodPost, "/v1/keys", map[string]string{"name": name}, &out)
	return out, err
}
