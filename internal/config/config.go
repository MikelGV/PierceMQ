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
}

var Env = initConfig()

func initConfig() Config {
	return Config{
		Port:        getEnv("PORT", "8080"),
		Host:        getEnv("HOST", "0.0.0.0"),
		RedisURI:    getEnv("REDIS_ADDR", getEnv("RedisURI", "redis://1234567890ca@localhost:6379/0")),
		DB_URL:      getEnv("DB_URL", "postgres://admin:admin@localhost:6432/piercemq?sslmode=disable"),
		DB_READ_URL: getEnv("DB_READ_URL", "postgres://admin:admin@localhost:6432/piercemq_ro?sslmode=disable"),
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
