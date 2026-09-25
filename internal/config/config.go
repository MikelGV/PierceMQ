package config

import (
	"os"
	"strconv"
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
}

var Env = initConfig()

func initConfig() Config {
	return Config{
		Port:                    getEnv("PORT", "8080"),
		Host:                    getEnv("HOST", "0.0.0.0"),
		RedisURI:                getEnv("REDIS_ADDR", getEnv("RedisURI", "redis://:1234567890ca@localhost:6379/0")),
		DB_URL:                  getEnv("DB_URL", "postgres://admin:admin@localhost:6432/piercemq?sslmode=disable"),
		DB_READ_URL:             getEnv("DB_READ_URL", "postgres://admin:admin@localhost:6432/piercemq_ro?sslmode=disable"),
		JWTSecret:               getEnv("JWT_SECRET", "dev-only-change-me"),
		JWTTTLHours:             getIntEnv("JWT_TTL_HOURS", 24),
		SchedPollSeconds:        getIntEnv("SCHED_POLL_SEC", 10),
		SchedBatchSize:          getIntEnv("SCHED_BATCH", 100),
		ReaperPollSeconds:       getIntEnv("REAPER_POLL_SEC", 30),
		ReaperBatchSize:         getIntEnv("REAPER_BATCH", 100),
		ReaperStaleSeconds:      getIntEnv("REAPER_STALE_SEC", 90),
		ReaperSweepAfterSeconds: getIntEnv("REAPER_SWEEP_AFTER_SEC", 300),
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
