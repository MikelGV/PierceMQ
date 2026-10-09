package task_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/task/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envWith(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

// --- email ---

func TestParseEmailPayload(t *testing.T) {
	job, err := handlers.ParseEmailPayload(map[string]any{
		"to": "a@example.com", "from": "b@example.com", "subject": "hi",
	})
	require.NoError(t, err)
	assert.Equal(t, "a@example.com", job.To)

	_, err = handlers.ParseEmailPayload(map[string]any{"from": "b@example.com"})
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err), "missing field must be permanent")

	_, err = handlers.ParseEmailPayload(map[string]any{"to": "not-an-email", "from": "b@example.com"})
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err), "bad address must be permanent")
}

func TestEmailDryRunByDefault(t *testing.T) {
	cfg := handlers.EmailConfigFromEnv(envWith(map[string]string{}))
	assert.True(t, cfg.DryRun, "no SMTP_HOST must mean dry-run")

	sender := handlers.SMTPSender{Cfg: cfg}
	id, err := sender.Send(context.Background(), handlers.EmailJob{
		To: "a@example.com", From: "b@example.com", JobID: "job-1",
	})
	require.NoError(t, err)
	assert.Contains(t, id, "dry-run:job-1")
}

func TestEmailRefusesBadHostTransiently(t *testing.T) {
	cfg := handlers.EmailConfigFromEnv(envWith(map[string]string{
		"SMTP_HOST": "127.0.0.1", "SMTP_PORT": "1", "SMTP_TIMEOUT_SEC": "1",
	}))
	sender := handlers.SMTPSender{Cfg: cfg}
	_, err := sender.Send(context.Background(), handlers.EmailJob{
		To: "a@example.com", From: "b@example.com",
	})
	require.Error(t, err)
	assert.False(t, handlers.IsPermanent(err), "dial failure must be retryable")
}

// --- file ---

func TestParseFilePayloadLegacyAndNew(t *testing.T) {
	legacy, err := handlers.ParseFilePayload(map[string]any{"filename": "report.pdf"})
	require.NoError(t, err)
	assert.True(t, legacy.Legacy)

	_, err = handlers.ParseFilePayload(map[string]any{"src": "a.txt"})
	require.Error(t, err, "src without dst must fail")

	_, err = handlers.ParseFilePayload(map[string]any{"src": "a.txt", "dst": "b.txt", "op": "frobnicate"})
	require.Error(t, err, "unknown op must fail")

	_, err = handlers.ParseFilePayload(map[string]any{"src": "s3://b", "dst": "b.txt"})
	require.Error(t, err, "malformed s3 url must fail")
	assert.True(t, handlers.IsPermanent(err))
}

func TestFileLocalCopyRoundTrip(t *testing.T) {
	base := t.TempDir()
	proc := handlers.NewProcessor(handlers.FileConfig{
		BaseDir: base, MaxBytes: 1 << 20,
	})
	require.NoError(t, proc.Local.Put(context.Background(), "in/hello.txt", []byte("hello piercemq")))

	res, err := proc.Process(context.Background(), handlers.FileJob{Src: "in/hello.txt", Dst: "out/copy.txt", Op: "copy"})
	require.NoError(t, err)
	assert.Contains(t, res, "copied 14 bytes")

	got, err := os.ReadFile(filepath.Join(base, "out", "copy.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello piercemq", string(got))

	res, err = proc.Process(context.Background(), handlers.FileJob{Src: "in/hello.txt", Op: "sha256"})
	require.NoError(t, err)
	assert.Contains(t, res, "sha256")
}

func TestFileTraversalRejected(t *testing.T) {
	proc := handlers.NewProcessor(handlers.FileConfig{BaseDir: t.TempDir(), MaxBytes: 1 << 20})
	_, err := proc.Process(context.Background(), handlers.FileJob{Src: "../evil.txt", Dst: "ok.txt"})
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err))

	err = proc.Local.Put(context.Background(), "/absolute.txt", []byte("x"))
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err))
}

func TestFileSizeCapEnforced(t *testing.T) {
	proc := handlers.NewProcessor(handlers.FileConfig{BaseDir: t.TempDir(), MaxBytes: 4})
	err := proc.Local.Put(context.Background(), "big.txt", []byte("way too long"))
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err))
}

func TestS3CopyAgainstTestServer(t *testing.T) {
	var gotPut []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("s3-bytes"))
		case http.MethodPut:
			buf := make([]byte, 64)
			n, _ := r.Body.Read(buf)
			gotPut = buf[:n]
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	s := &handlers.S3Storage{Cfg: handlers.FileConfig{
		S3Endpoint: srv.URL, S3AccessKey: "ak", S3SecretKey: "sk",
		S3Region: "us-east-1", MaxBytes: 1 << 20,
	}, Client: srv.Client()}

	data, err := s.Get(context.Background(), "s3://bucket/key.txt")
	require.NoError(t, err)
	assert.Equal(t, "s3-bytes", string(data))

	require.NoError(t, s.Put(context.Background(), "s3://bucket/out.txt", []byte("s3-bytes")))
	assert.Equal(t, "s3-bytes", string(gotPut))

	// Signed requests must carry an Authorization header.
	reqSeen := ""
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqSeen = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv2.Close()
	s2 := &handlers.S3Storage{Cfg: handlers.FileConfig{
		S3Endpoint: srv2.URL, S3AccessKey: "ak", S3SecretKey: "sk",
		S3Region: "us-east-1", MaxBytes: 1 << 20,
	}, Client: srv2.Client()}
	_, err = s2.Get(context.Background(), "s3://b/k")
	require.NoError(t, err)
	assert.Contains(t, reqSeen, "AWS4-HMAC-SHA256")
}

// --- exec ---

func TestParseExecPayload(t *testing.T) {
	_, err := handlers.ParseExecPayload(map[string]any{})
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err))

	_, err = handlers.ParseExecPayload(map[string]any{"command": "ls", "args": "not-an-array"})
	require.Error(t, err)

	_, err = handlers.ParseExecPayload(map[string]any{"command": "/bin/ls"})
	require.Error(t, err, "path in command must be rejected")

	job, err := handlers.ParseExecPayload(map[string]any{"command": "ls", "args": []any{"-la"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"-la"}, job.Args)
}

func TestExecDenyAll(t *testing.T) {
	r := handlers.Runner{Cfg: handlers.ExecConfig{}}
	_, err := r.Run(context.Background(), handlers.ExecJob{Command: "ls"})
	require.Error(t, err)
	assert.True(t, handlers.IsPermanent(err), "deny-all must be permanent")
}

func TestExecAllowlistedTrue(t *testing.T) {
	r := handlers.Runner{Cfg: handlers.ExecConfig{
		Allowlist: []string{"true"}, WorkDir: t.TempDir(), MaxOutput: 1024,
	}}
	res, err := r.Run(context.Background(), handlers.ExecJob{Command: "true"})
	require.NoError(t, err)
	assert.Contains(t, res, "ok")
}

func TestExecTimeoutKillsProcess(t *testing.T) {
	r := handlers.Runner{Cfg: handlers.ExecConfig{
		Allowlist: []string{"sleep"}, WorkDir: t.TempDir(), MaxOutput: 1024,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := r.Run(ctx, handlers.ExecJob{Command: "sleep", Args: []string{"10"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}
