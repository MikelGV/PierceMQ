package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// sessionCookie carries the API JWT. HttpOnly + SameSite=Lax (set in
// auth.go); the browser never exposes it to JS.
const sessionCookie = "pmq_jwt"

// apiError is a non-2xx API response.
type apiError struct {
	Status int
	Msg    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("api: %d %s", e.Status, e.Msg)
}

// tokenFromRequest returns the session JWT, if present.
func tokenFromRequest(r *http.Request) (string, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return "", false
	}
	return c.Value, true
}

// apiDo performs a JSON request against the API with the session bearer.
// A nil body sends no payload. A non-nil out decodes the JSON response.
// Non-2xx responses return *apiError (401 included: callers redirect to
// login and clear the cookie).
func (s *Server) apiDo(r *http.Request, method, path string, body any, out any) (int, error) {
	return s.apiDoWithHeaders(r, method, path, nil, body, out)
}

// apiDoWithHeaders is apiDo plus extra request headers (e.g. Idempotency-Key).
func (s *Server) apiDoWithHeaders(r *http.Request, method, path string, headers map[string]string, body any, out any) (int, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, fmt.Errorf("web: encode: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(r.Context(), method, s.apiBase+path, &buf)
	if err != nil {
		return 0, fmt.Errorf("web: request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if token, ok := tokenFromRequest(r); ok {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("web: api unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("web: read: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody map[string]any
		_ = json.Unmarshal(raw, &errBody)
		msg, _ := errBody["error"].(string)
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return resp.StatusCode, &apiError{Status: resp.StatusCode, Msg: msg}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("web: decode: %w", err)
		}
	}
	return resp.StatusCode, nil
}

// apiStatusOf unwraps the HTTP status from an apiDo error (0 when the API
// was unreachable or the failure was local).
func apiStatusOf(err error) int {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status
	}
	return 0
}
