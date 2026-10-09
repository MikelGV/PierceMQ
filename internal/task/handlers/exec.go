package handlers

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// ExecConfig configures command execution.
//
// SECURITY MODEL: no shell is ever involved (exec.CommandContext with an
// argv array, never `sh -c`), the binary basename must be an exact member
// of Allowlist, and execution happens in WorkDir. An empty allowlist means
// deny-all: every exec job fails Permanent (no point retrying), which is
// also the pre-handler behaviour (commands were never executed).
type ExecConfig struct {
	Allowlist []string
	WorkDir   string
	MaxOutput int // captured output truncation (bytes)
}

// ExecConfigFromEnv builds an ExecConfig from EXEC_ALLOWLIST (comma-separated
// binary names), EXEC_WORKDIR and EXEC_MAX_OUTPUT.
func ExecConfigFromEnv(getenv func(string) string) ExecConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	workdir := strings.TrimSpace(getenv("EXEC_WORKDIR"))
	if workdir == "" {
		workdir = filepath.Join(os.TempDir(), "piercemq-exec")
	}
	maxOut := 4096
	if v := strings.TrimSpace(getenv("EXEC_MAX_OUTPUT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOut = n
		}
	}
	return ExecConfig{
		Allowlist: splitCSV(getenv("EXEC_ALLOWLIST")),
		WorkDir:   workdir,
		MaxOutput: maxOut,
	}
}

// ExecJob is a validated exec_processing payload.
type ExecJob struct {
	Command string
	Args    []string
	WorkDir string // optional per-job subdir under the sandbox (no absolute, no ..)
}

// ParseExecPayload validates a decoded exec payload. command is required;
// args must be a JSON array of strings when present (never a shell string).
func ParseExecPayload(payload map[string]any) (ExecJob, error) {
	cmd, ok := strField(payload, "command", "cmd", "binary")
	if !ok {
		return ExecJob{}, PermanentMsg("exec job missing required field: command")
	}
	// Reject path traversal in the binary itself; allowlist matches basenames.
	if strings.Contains(cmd, "/") || strings.Contains(cmd, "\\") {
		if base := filepath.Base(cmd); base != cmd {
			return ExecJob{}, PermanentMsg("exec command must be a bare binary name, got %q", cmd)
		}
	}
	var args []string
	if raw, ok := payload["args"]; ok && raw != nil {
		arr, ok := raw.([]any)
		if !ok {
			return ExecJob{}, PermanentMsg("exec job field args must be an array of strings")
		}
		for i, a := range arr {
			s, ok := a.(string)
			if !ok {
				return ExecJob{}, PermanentMsg("exec job args[%d] must be a string", i)
			}
			args = append(args, s)
		}
	}
	workdir, _ := strField(payload, "workdir", "dir")
	if workdir != "" {
		if filepath.IsAbs(workdir) || workdir == ".." || strings.HasPrefix(workdir, "../") {
			return ExecJob{}, PermanentMsg("exec workdir must be relative without .. escape")
		}
	}
	return ExecJob{Command: cmd, Args: args, WorkDir: workdir}, nil
}

// Runner executes allowlisted binaries.
type Runner struct {
	Cfg ExecConfig
}

// Run executes the job. Allowlist misses are Permanent; start failures
// (missing binary) are Permanent; non-zero exits and signal kills are plain
// errors carrying the truncated output (retryable per max_retries); ctx
// timeout (JOB_TIMEOUT_SEC enforced by the worker) aborts the process.
func (r Runner) Run(ctx context.Context, job ExecJob) (string, error) {
	allowed := false
	for _, a := range r.Cfg.Allowlist {
		if job.Command == a || filepath.Base(a) == job.Command {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", PermanentMsg("exec command %q not in EXEC_ALLOWLIST", job.Command)
	}
	dir := r.Cfg.WorkDir
	if job.WorkDir != "" {
		dir = filepath.Join(dir, filepath.Clean(job.WorkDir))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("exec workdir: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, job.Command, job.Args...)
	cmd.Dir = dir
	// Minimal, non-inheriting environment: no secrets leak into children,
	// deterministic PATH for allowlisted binaries.
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/nonexistent"}
	var buf bytes.Buffer
	max := r.Cfg.MaxOutput
	if max <= 0 {
		max = 4096
	}
	cmd.Stdout = &cappedWriter{W: &buf, Max: max}
	cmd.Stderr = &cappedWriter{W: &buf, Max: max}

	if err := cmd.Start(); err != nil {
		return "", PermanentMsg("exec start %q: %v", job.Command, err)
	}
	err := cmd.Wait()
	out := truncateString(buf.String(), max)
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return "", fmt.Errorf("exec %q timed out: output: %s", job.Command, out)
	case err != nil:
		return "", fmt.Errorf("exec %q failed: %v: output: %s", job.Command, err, out)
	default:
		if out == "" {
			return fmt.Sprintf("exec %s ok (no output)", job.Command), nil
		}
		return fmt.Sprintf("exec %s ok: %s", job.Command, out), nil
	}
}

func truncateString(s string, max int) string {
	if len(s) <= max {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(s[:max]) + "…[truncated]"
}

// cappedWriter caps total bytes retained (rest discarded, no error).
type cappedWriter struct {
	W   *bytes.Buffer
	Max int
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	room := c.Max - c.W.Len()
	if room <= 0 {
		return len(p), nil
	}
	if len(p) > room {
		p = p[:room]
	}
	return c.W.Write(p)
}
