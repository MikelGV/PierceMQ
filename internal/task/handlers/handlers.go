// Package handlers implements the real job-type handlers behind
// Worker.ProcessJobs: SMTP email delivery, file copy/verify (local FS +
// S3-compatible object storage), and allowlisted command execution.
//
// Design notes:
//   - Stdlib only (net/smtp, net/http, os/exec) — no new module deps.
//   - Every handler reads config from env at call time with safe defaults
//     (dry-run / deny-all), so local runs and unit tests work without infra.
//   - PermanentError marks failures that must not be retried (bad address,
//     allowlist deny, path traversal). Callers use IsPermanent to decide.
//     NOTE: the current FailOrRetry path counts every failure against
//     max_retry; a future improvement is to fail permanent errors fast.
package handlers

import (
	"errors"
	"fmt"
	"strings"
)

// PermanentError wraps a failure that retrying will never fix.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent marks err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		return err
	}
	return &PermanentError{Err: err}
}

// PermanentMsg is Permanent(fmt.Errorf(format, args...)).
func PermanentMsg(format string, args ...any) error {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// getenvOr returns env[key] or fallback when unset/blank.
func getenvOr(getenv func(string) string, key, fallback string) string {
	if getenv == nil {
		return fallback
	}
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}

// splitCSV parses a comma-separated env var into trimmed non-empty items.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// strField extracts a string field from a decoded JSON payload map,
// accepting plain strings only.
func strField(payload map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		v, ok := payload[k]
		if !ok || v == nil {
			continue
		}
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			return s, true
		}
	}
	return "", false
}
