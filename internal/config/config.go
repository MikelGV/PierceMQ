package config

import (
	"os"
	"strconv"

	"github.com/MikelGV/PierceMQ/internal/broker"
)

type Config struct {
	Port        string
	Host        string
	RedisURI    string
	PSQLURI     string
	DB_URL      string
	DB_READ_URL string
	JWTSecret   string
	JWTTTLHours int64
	// RedisSentinels is the optional comma-separated host:port list
	// (REDIS_SENTINELS) enabling Sentinel failover (§11.3.2). Empty means
	// single-node mode.
	RedisSentinels string
	// RedisMasterName is the Sentinel master name (REDIS_MASTER).
	RedisMasterName string
	// StreamMaxLen caps each Redis stream via approximate MAXLEN trimming
	// (§10.3 retention ≈ throughput × acceptable lag). Zero/negative falls
	// back to broker.DefaultStreamMaxLen.
	StreamMaxLen int64
	// SchedPollSeconds is the scheduler poll cadence (§7.5: 10s).
	SchedPollSeconds int64
	// SchedBatchSize caps jobs promoted per scheduler tick.
	SchedBatchSize int64
	// ReaperPollSeconds is the reaper tick cadence (§12.2-12.3).
	ReaperPollSeconds int64
	// ReaperBatchSize caps jobs reclaimed/swept per reaper tick.
	ReaperBatchSize int64
	// ReaperStaleSeconds is the heartbeat timeout: running jobs with
	// heartbeat_at older than this are reclaimed (§12.2: 90s).
	ReaperStaleSeconds int64
	// ReaperSweepAfterSeconds is how old a pending job must be before the
	// stale-pending sweeper re-dispatches it.
	ReaperSweepAfterSeconds int64
	// JobTimeoutSeconds caps handler execution (§3.2: 5 minutes max).
	// Zero/negative falls back to DefaultJobTimeoutSeconds.
	JobTimeoutSeconds int64
	// RetentionDays bounds terminal-job retention (§10.4: 4 days).
	// Zero/negative disables the purger.
	RetentionDays int64
	// RetentionPollSeconds is the purger cadence. Non-positive gets default.
	RetentionPollSeconds int64
	// RetentionBatchSize caps rows purged per cycle.
	RetentionBatchSize int64
	// RateLimitRPS is the per-caller sustained request rate (§7.6, §12.5).
	// Zero/negative falls back to DefaultRateLimitRPS.
	RateLimitRPS int64
	// RateLimitBurst is the per-caller token bucket size. Zero/negative
	// falls back to DefaultRateLimitBurst.
	RateLimitBurst int64
	// --- Job handler settings (internal/task/handlers) ---
	// Email: SMTP_HOST empty (or EMAIL_DRY_RUN=1) means validate-only.
	SMTPHost    string
	SMTPPort    int64
	SMTPUser    string
	SMTPFrom    string
	SMTPTimeout int64
	EmailDryRun bool
	// Files: local sandbox + optional S3-compatible endpoint.
	FileBaseDir string
	FileMaxMB   int64
	S3Endpoint  string
	S3AccessKey string
	S3Region    string
	S3UseSSL    bool
	// Exec: comma-separated allowlist of bare binary names. Empty = deny-all.
	ExecAllowlist string
	ExecWorkDir   string
	ExecMaxOutput int64
}

var Env = initConfig()

// Default per-caller rate limits (§7.6, §12.5): 20 rps sustained, 40 burst.
const (
	DefaultRateLimitRPS   = 20
	DefaultRateLimitBurst = 40
)

func initConfig() Config {
	return Config{
		Port:                    getEnv("PORT", "8080"),
		Host:                    getEnv("HOST", "0.0.0.0"),
		RedisURI:                getEnv("REDIS_ADDR", getEnv("RedisURI", "redis://:1234567890ca@localhost:6379/0")),
		DB_URL:                  getEnv("DB_URL", "postgres://admin:admin@localhost:6432/piercemq?sslmode=disable"),
		DB_READ_URL:             getEnv("DB_READ_URL", "postgres://admin:admin@localhost:6432/piercemq_ro?sslmode=disable"),
		JWTSecret:               getEnv("JWT_SECRET", "dev-only-change-me"),
		JWTTTLHours:             getIntEnv("JWT_TTL_HOURS", 24),
		RedisSentinels:          getEnv("REDIS_SENTINELS", ""),
		RedisMasterName:         getEnv("REDIS_MASTER", broker.DefaultSentinelMaster),
		StreamMaxLen:            getIntEnv("STREAM_MAX_LEN", broker.DefaultStreamMaxLen),
		SchedPollSeconds:        getIntEnv("SCHED_POLL_SEC", 10),
		SchedBatchSize:          getIntEnv("SCHED_BATCH", 100),
		ReaperPollSeconds:       getIntEnv("REAPER_POLL_SEC", 30),
		ReaperBatchSize:         getIntEnv("REAPER_BATCH", 100),
		ReaperStaleSeconds:      getIntEnv("REAPER_STALE_SEC", 90),
		ReaperSweepAfterSeconds: getIntEnv("REAPER_SWEEP_AFTER_SEC", 60),
		JobTimeoutSeconds:       getIntEnv("JOB_TIMEOUT_SEC", 300),
		RetentionDays:           getIntEnv("RETENTION_DAYS", 4),
		RetentionPollSeconds:    getIntEnv("RETENTION_POLL_SEC", 3600),
		RetentionBatchSize:      getIntEnv("RETENTION_BATCH", 1000),
		RateLimitRPS:            getIntEnv("RATE_LIMIT_RPS", DefaultRateLimitRPS),
		RateLimitBurst:          getIntEnv("RATE_LIMIT_BURST", DefaultRateLimitBurst),
		SMTPHost:                getEnv("SMTP_HOST", ""),
		SMTPPort:                getIntEnv("SMTP_PORT", 587),
		SMTPUser:                getEnv("SMTP_USER", ""),
		SMTPFrom:                getEnv("SMTP_FROM", ""),
		SMTPTimeout:             getIntEnv("SMTP_TIMEOUT_SEC", 15),
		EmailDryRun:             getEnv("EMAIL_DRY_RUN", "") == "1",
		FileBaseDir:             getEnv("FILE_BASE_DIR", ""),
		FileMaxMB:               getIntEnv("FILE_MAX_MB", 100),
		S3Endpoint:              getEnv("S3_ENDPOINT", ""),
		S3AccessKey:             getEnv("S3_ACCESS_KEY", ""),
		S3Region:                getEnv("S3_REGION", "us-east-1"),
		S3UseSSL:                getEnv("S3_USE_SSL", "") == "1",
		ExecAllowlist:           getEnv("EXEC_ALLOWLIST", ""),
		ExecWorkDir:             getEnv("EXEC_WORKDIR", ""),
		ExecMaxOutput:           getIntEnv("EXEC_MAX_OUTPUT", 4096),
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}

	return fallback
}

func getIntEnv(key string, fallback int64) int64 {
	if val, ok := os.LookupEnv(key); ok {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fallback
		}

		return i
	}

	return fallback
}
