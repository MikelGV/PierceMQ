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
type EnqueueRequest struct {
	Type           string         `json:"type"`
	QueueName      string         `json:"queue_name,omitempty"`
	Payload        map[string]any `json:"payload"`
	Priority       int16          `json:"priority,omitempty"`
	MaxRetry       int16          `json:"max_retry,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	ScheduledAt    string         `json:"scheduled_at,omitempty"`
}

// Enqueue performs the DB-first dual write via the API.
func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (map[string]any, error) {
	var out map[string]any
	err := c.do(ctx, http.MethodPost, "/v1/jobs", req, &out)
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

// CreateAPIKey mints a service key (plaintext returned once).
func (c *Client) CreateAPIKey(ctx context.Context, name string) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodPost, "/v1/keys", map[string]string{"name": name}, &out)
	return out, err
}
