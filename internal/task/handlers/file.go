package handlers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FileJob is a validated file_processing payload.
//
// New-style (real I/O):  {src, dst, op} where src/dst are
// s3://bucket/key, file://relative/path or bare relative paths.
// op is ""|"copy" (default) or "sha256".
//
// Legacy-style (validate-only, no I/O): {filename|path, ...}. Kept so jobs
// submitted against the old stub contract keep succeeding; returns a
// descriptive string without touching disk or network.
type FileJob struct {
	Src string
	Dst string
	Op  string

	Legacy bool   // true when payload used filename/path without src+dst
	Hint   string // filename/path hint for legacy result strings
}

// FileConfig configures file processing.
type FileConfig struct {
	BaseDir  string // sandbox root for local paths (FILE_BASE_DIR)
	MaxBytes int64  // per-file cap (FILE_MAX_MB, default 100MB)

	S3Endpoint  string // e.g. http://minio:9000 (S3_ENDPOINT, "" = local-only)
	S3AccessKey string
	S3SecretKey string
	S3Region    string // default us-east-1
	S3UseSSL    bool
	HTTPTimeout time.Duration
}

// FileConfigFromEnv builds a FileConfig from FILE_BASE_DIR/FILE_MAX_MB and
// S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY/S3_REGION/S3_USE_SSL.
func FileConfigFromEnv(getenv func(string) string) FileConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	maxMB := int64(100)
	if v := strings.TrimSpace(getenv("FILE_MAX_MB")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxMB = n
		}
	}
	timeout := 30 * time.Second
	if v := strings.TrimSpace(getenv("S3_TIMEOUT_SEC")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}
	useSSL := false
	if v := strings.TrimSpace(getenv("S3_USE_SSL")); v == "1" || strings.EqualFold(v, "true") {
		useSSL = true
	}
	base := strings.TrimSpace(getenv("FILE_BASE_DIR"))
	if base == "" {
		base = filepath.Join(os.TempDir(), "piercemq-files")
	}
	return FileConfig{
		BaseDir:     base,
		MaxBytes:    maxMB << 20,
		S3Endpoint:  strings.TrimRight(strings.TrimSpace(getenv("S3_ENDPOINT")), "/"),
		S3AccessKey: strings.TrimSpace(getenv("S3_ACCESS_KEY")),
		S3SecretKey: getenv("S3_SECRET_KEY"),
		S3Region:    firstNonEmpty(strings.TrimSpace(getenv("S3_REGION")), "us-east-1"),
		S3UseSSL:    useSSL,
		HTTPTimeout: timeout,
	}
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// ParseFilePayload validates a decoded file payload.
func ParseFilePayload(payload map[string]any) (FileJob, error) {
	src, hasSrc := strField(payload, "src", "source", "url")
	dst, hasDst := strField(payload, "dst", "dest", "destination")
	op, _ := strField(payload, "op", "operation")
	op = strings.ToLower(strings.TrimSpace(op))

	if hasSrc || hasDst {
		if !hasSrc {
			return FileJob{}, PermanentMsg("file job missing required field: src")
		}
		if !hasDst && op != "sha256" {
			return FileJob{}, PermanentMsg("file job missing required field: dst")
		}
		switch op {
		case "", "copy", "sha256":
		default:
			return FileJob{}, PermanentMsg("file job unknown op %q (want copy|sha256)", op)
		}
		if strings.HasPrefix(src, "s3://") || strings.HasPrefix(dst, "s3://") {
			if err := parseS3URL(firstNonEmpty(src, dst)); err != nil {
				return FileJob{}, err
			}
		}
		return FileJob{Src: src, Dst: dst, Op: op}, nil
	}

	// Legacy stub contract: filename or path present, no I/O requested.
	if hint, ok := strField(payload, "filename", "path"); ok {
		return FileJob{Legacy: true, Hint: hint}, nil
	}
	return FileJob{}, PermanentMsg("file job missing required field: filename or path (legacy) or src+dst")
}

// Storage fetches and stores blob bytes.
type Storage interface {
	Get(ctx context.Context, ref string) ([]byte, error)
	Put(ctx context.Context, ref string, data []byte) error
}

// Processor routes file jobs to the right Storage by URL scheme.
type Processor struct {
	Cfg   FileConfig
	Local *LocalFSStorage
	S3    *S3Storage
}

// NewProcessor builds a Processor from cfg.
func NewProcessor(cfg FileConfig) *Processor {
	return &Processor{
		Cfg:   cfg,
		Local: &LocalFSStorage{BaseDir: cfg.BaseDir, MaxBytes: cfg.MaxBytes},
		S3:    &S3Storage{Cfg: cfg, Client: &http.Client{Timeout: cfg.HTTPTimeout}},
	}
}

func (p *Processor) storageFor(ref string) (Storage, error) {
	if strings.HasPrefix(ref, "s3://") {
		if p.Cfg.S3Endpoint == "" {
			return nil, PermanentMsg("file job references s3:// URL but S3_ENDPOINT is not configured")
		}
		return p.S3, nil
	}
	return p.Local, nil
}

// Process executes the file job and returns a human-readable result.
// Result strings (never raw bytes) are what the worker logs and — once a
// result column exists — persists.
func (p *Processor) Process(ctx context.Context, job FileJob) (string, error) {
	if job.Legacy {
		if job.Hint == "" {
			return "file validated (no src/dst, no I/O)", nil
		}
		return fmt.Sprintf("file validated %s (no src/dst, no I/O)", job.Hint), nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	srcStore, err := p.storageFor(job.Src)
	if err != nil {
		return "", err
	}
	data, err := srcStore.Get(ctx, job.Src)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	hexSum := hex.EncodeToString(sum[:])
	if job.Op == "sha256" {
		return fmt.Sprintf("sha256 %s %s (%d bytes)", shortHex(hexSum), job.Src, len(data)), nil
	}
	dstStore, err := p.storageFor(job.Dst)
	if err != nil {
		return "", err
	}
	if err := dstStore.Put(ctx, job.Dst, data); err != nil {
		return "", err
	}
	return fmt.Sprintf("copied %d bytes %s -> %s sha256:%s", len(data), job.Src, job.Dst, shortHex(hexSum)), nil
}

func shortHex(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// ---------------------------------------------------------------------------
// Local filesystem storage (sandboxed)
// ---------------------------------------------------------------------------

// LocalFSStorage reads/writes under BaseDir. Bare relative paths and
// file:// URLs are resolved inside the sandbox; absolute paths and ..
// escapes are rejected (PermanentError).
type LocalFSStorage struct {
	BaseDir  string
	MaxBytes int64
}

func (l *LocalFSStorage) resolve(ref string) (string, error) {
	p := strings.TrimSpace(ref)
	p = strings.TrimPrefix(p, "file://")
	if filepath.IsAbs(p) {
		return "", PermanentMsg("file path must be relative to FILE_BASE_DIR, got absolute %q", ref)
	}
	clean := filepath.Clean(p)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", PermanentMsg("file path escapes FILE_BASE_DIR: %q", ref)
	}
	base := l.BaseDir
	if base == "" {
		base = os.TempDir()
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return "", fmt.Errorf("resolve base dir: %w", err)
	}
	joined := filepath.Join(absBase, clean)
	rel, err := filepath.Rel(absBase, joined)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", PermanentMsg("file path escapes FILE_BASE_DIR: %q", ref)
	}
	return joined, nil
}

// Get reads a sandboxed file, capped at MaxBytes.
func (l *LocalFSStorage) Get(ctx context.Context, ref string) ([]byte, error) {
	path, err := l.resolve(ref)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", ref, err)
	}
	defer f.Close()
	max := l.MaxBytes
	if max <= 0 {
		max = 100 << 20
	}
	data, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ref, err)
	}
	if int64(len(data)) > max {
		return nil, PermanentMsg("file %s exceeds %d bytes cap", ref, max)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// Put writes atomically (temp file + rename) inside the sandbox.
func (l *LocalFSStorage) Put(ctx context.Context, ref string, data []byte) error {
	path, err := l.resolve(ref)
	if err != nil {
		return err
	}
	max := l.MaxBytes
	if max <= 0 {
		max = 100 << 20
	}
	if int64(len(data)) > max {
		return PermanentMsg("file %s exceeds %d bytes cap", ref, max)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir for %s: %w", ref, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", ref, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", ref, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", ref, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename %s: %w", ref, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// S3-compatible storage (SigV4, path-style, stdlib only)
// ---------------------------------------------------------------------------

// S3Storage speaks the S3 REST API (path-style) against any S3-compatible
// endpoint (AWS S3, MinIO). Credentials sign every request with SigV4;
// without credentials it falls back to unsigned requests (public buckets).
type S3Storage struct {
	Cfg    FileConfig
	Client *http.Client
}

func (s *S3Storage) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func parseS3URL(ref string) error {
	rest := strings.TrimPrefix(ref, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return PermanentMsg("invalid s3 URL %q (want s3://bucket/key)", ref)
	}
	return nil
}

func splitS3URL(ref string) (bucket, key string, err error) {
	if err := parseS3URL(ref); err != nil {
		return "", "", err
	}
	rest := strings.TrimPrefix(ref, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	return parts[0], parts[1], nil
}

// Get downloads an object.
func (s *S3Storage) Get(ctx context.Context, ref string) ([]byte, error) {
	bucket, key, err := splitS3URL(ref)
	if err != nil {
		return nil, err
	}
	req, err := s.signedRequest(ctx, http.MethodGet, bucket, key, nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 GET %s: %w", ref, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return nil, PermanentMsg("s3 GET %s -> %d %s", ref, resp.StatusCode, strings.TrimSpace(string(snippet)))
		}
		return nil, fmt.Errorf("s3 GET %s -> %d %s", ref, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	max := s.Cfg.MaxBytes
	if max <= 0 {
		max = 100 << 20
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, fmt.Errorf("s3 read %s: %w", ref, err)
	}
	if int64(len(data)) > max {
		return nil, PermanentMsg("s3 object %s exceeds %d bytes cap", ref, max)
	}
	return data, nil
}

// Put uploads an object (single PUT; multipart upload is out of scope).
func (s *S3Storage) Put(ctx context.Context, ref string, data []byte) error {
	bucket, key, err := splitS3URL(ref)
	if err != nil {
		return err
	}
	max := s.Cfg.MaxBytes
	if max <= 0 {
		max = 100 << 20
	}
	if int64(len(data)) > max {
		return PermanentMsg("s3 object %s exceeds %d bytes cap", ref, max)
	}
	req, err := s.signedRequest(ctx, http.MethodPut, bucket, key, data, "application/octet-stream")
	if err != nil {
		return err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return fmt.Errorf("s3 PUT %s: %w", ref, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return PermanentMsg("s3 PUT %s -> %d", ref, resp.StatusCode)
		}
		return fmt.Errorf("s3 PUT %s -> %d", ref, resp.StatusCode)
	}
	return nil
}

func (s *S3Storage) signedRequest(ctx context.Context, method, bucket, key string, body []byte, contentType string) (*http.Request, error) {
	endpoint := strings.TrimRight(s.Cfg.S3Endpoint, "/")
	if endpoint == "" {
		return nil, PermanentMsg("S3_ENDPOINT is not configured")
	}
	u, err := url.Parse(endpoint + "/" + bucket + "/" + encodeKey(key))
	if err != nil {
		return nil, fmt.Errorf("s3 url: %w", err)
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return nil, fmt.Errorf("s3 request: %w", err)
	}
	payloadHash := sha256Hex(body)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if s.Cfg.S3AccessKey == "" {
		return req, nil // unsigned: public buckets / open MinIO policies
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	region := s.Cfg.S3Region
	if region == "" {
		region = "us-east-1"
	}
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + req.URL.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonicalRequest := method + "\n" +
		"/" + bucket + "/" + encodeKey(key) + "\n" +
		"\n" + // query string (none)
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		payloadHash
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))
	signingKey := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256([]byte("AWS4"+s.Cfg.S3SecretKey), dateStamp), region), "s3"), "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.Cfg.S3AccessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
	return req, nil
}

func encodeKey(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
