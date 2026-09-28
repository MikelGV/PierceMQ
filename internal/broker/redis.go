package broker

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	Conn *redis.Client
	// Jobs is the worker-side PG handle for claim/complete. Nil means
	// Redis-only mode (tests, or pools without a database); the consume
	// path then dispatches stream payloads without DB transitions.
	Jobs JobStore
	// MaxLen caps stream length via approximate MAXLEN trimming (§10.3
	// retention: cap ≈ max_throughput_per_sec × max_acceptable_lag_seconds,
	// tuned per deployment, not per stream, for now). Zero resolves to
	// DefaultStreamMaxLen; every XADD path (dispatch, retry, DLQ) applies it
	// so no stream — including dead-letter streams with no consumer — can
	// grow without bound under backlog or consumer outage.
	MaxLen int64
}

// DefaultStreamMaxLen bounds each stream when RedisStore.MaxLen is unset.
const DefaultStreamMaxLen = 50_000

// DefaultSentinelMaster is the Sentinel master name when REDIS_MASTER is unset.
const DefaultSentinelMaster = "mymaster"

// EffectiveMaxLen resolves the cap in force: the override when positive,
// DefaultStreamMaxLen otherwise. Never zero: streams are always capped.
func (rds *RedisStore) EffectiveMaxLen() int64 {
	if rds != nil && rds.MaxLen > 0 {
		return rds.MaxLen
	}
	return DefaultStreamMaxLen
}

// Re-export queue constants for backward compatibility; canonical source is internal/queue.
const (
	Email_high_stream = queue.EmailHighStream
	Email_low_stream  = queue.EmailLowStream
	File_high_stream  = queue.FileHighStream
	File_low_stream   = queue.FileLowStream
	Exec_high_stream  = queue.ExecHighStream
	Exec_low_stream   = queue.ExecLowStream

	Email_group_high = queue.EmailGroupHigh
	Email_group_low  = queue.EmailGroupLow
	File_group_high  = queue.FileGroupHigh
	File_group_low   = queue.FileGroupLow
	Exec_group_high  = queue.ExecGroupHigh
	Exec_group_low   = queue.ExecGroupLow
)

func Redis_Connect(rdsUrl string) (*RedisStore, error) {

	opt, err := redis.ParseURL(rdsUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing url: %s\n", err)
		return nil, err
	}

	rds := redis.NewClient(opt)

	if err := rds.Ping(context.Background()).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging redis: %s\n", err)
		return nil, fmt.Errorf("ping redis: %w\n", err)
	}
	if err := initStreams(rds); err != nil {
		return nil, err
	}

	return &RedisStore{
		Conn: rds,
	}, nil
}

// ParseSentinelAddrs parses a comma-separated host:port list (REDIS_SENTINELS
// env). Empty/blank entries are dropped; empty input yields nil, which means
// single-node mode.
func ParseSentinelAddrs(env string) []string {
	var out []string
	for _, part := range strings.Split(env, ",") {
		if addr := strings.TrimSpace(part); addr != "" {
			out = append(out, addr)
		}
	}
	return out
}

// FailoverOptionsFor builds go-redis sentinel options without any network
// I/O: the Redis password and DB come from the same redis:// URL as
// single-node mode, so credentials stay in one place.
func FailoverOptionsFor(master string, addrs []string, rdsUrl string) (*redis.FailoverOptions, error) {
	if master == "" {
		return nil, fmt.Errorf("sentinel: empty master name")
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("sentinel: no sentinel addresses")
	}
	opt, err := redis.ParseURL(rdsUrl)
	if err != nil {
		return nil, fmt.Errorf("sentinel: parse url: %w", err)
	}
	return &redis.FailoverOptions{
		MasterName:    master,
		SentinelAddrs: addrs,
		Password:      opt.Password,
		DB:            opt.DB,
	}, nil
}

// DialSentinel builds a Sentinel-managed failover client (§11.3.2) without
// any network I/O: the caller pings to verify.
func DialSentinel(master string, addrs []string, rdsUrl string) (*redis.Client, error) {
	opts, err := FailoverOptionsFor(master, addrs, rdsUrl)
	if err != nil {
		return nil, err
	}
	return redis.NewFailoverClient(opts), nil
}

// Redis_ConnectWithSentinel dials via Sentinel (automatic failover), pings
// to verify, and initializes streams/groups like Redis_Connect.
func Redis_ConnectWithSentinel(master string, addrs []string, rdsUrl string) (*RedisStore, error) {
	conn, err := DialSentinel(master, addrs, rdsUrl)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()).Err(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping redis via sentinel: %w", err)
	}
	if err := initStreams(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return &RedisStore{Conn: conn}, nil
}

// ConnectFromConfig selects the Redis transport: Sentinel failover when
// sentinelsEnv is non-empty, single-node otherwise. All services (api,
// worker, scheduler, reaper) dial through here so the topology flips with
// one env var and no code change.
func ConnectFromConfig(redisURL, sentinelsEnv, master string) (*RedisStore, error) {
	if addrs := ParseSentinelAddrs(sentinelsEnv); len(addrs) > 0 {
		if master == "" {
			master = DefaultSentinelMaster
		}
		return Redis_ConnectWithSentinel(master, addrs, redisURL)
	}
	return Redis_Connect(redisURL)
}

// initStreams creates the queue streams + consumer groups (idempotent via
// BUSYGROUP tolerance in InitConsumerGroupsAndStreams).
func initStreams(rds *redis.Client) error {
	streamsAndGroups := map[string]string{
		Email_high_stream: Email_group_high,
		Email_low_stream:  Email_group_low,
		File_high_stream:  File_group_high,
		File_low_stream:   File_group_low,
		Exec_high_stream:  Exec_group_high,
		Exec_low_stream:   Exec_group_low,
	}

	for stream, group := range streamsAndGroups {
		if err := InitConsumerGroupsAndStreams(context.Background(), stream, group, rds); err != nil {
			return fmt.Errorf("Error trying to initialize consumer %w\n", err)
		}
		continue
	}
	return nil
}
