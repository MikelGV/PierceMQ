package handlers

import (
	"context"
	"net/http"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/storage/jobs"
)

// watchedStreams are the queue streams summed into queue_depth. Canonical
// source is internal/queue; broker re-exports the same constants.
var watchedStreams = []string{
	queue.EmailHighStream,
	queue.EmailLowStream,
	queue.FileHighStream,
	queue.FileLowStream,
	queue.ExecHighStream,
	queue.ExecLowStream,
}

// NewStatsHandler serves GET /v1/stats (§9.8): per-status job counts from
// PostgreSQL plus live worker and queue-depth signals from Redis. Redis gaps
// degrade (zeros) rather than fail: PG counts are the source of truth.
func NewStatsHandler(store *jobs.JobsStore, rds *broker.RedisStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if _, ok := auth.UserIDFromContext(r.Context()); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		counts, err := store.JobStats(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "stats failed"})
			return
		}
		pendingAge, err := store.OldestPendingAgeSec(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "stats failed"})
			return
		}
		workers, depth := redisSignals(r.Context(), rds)
		writeJSON(w, http.StatusOK, map[string]any{
			"pending":               counts.Pending + counts.Queued,
			"queued":                counts.Queued,
			"processing":            counts.Running,
			"completed":             counts.Completed,
			"failed":                counts.Failed,
			"scheduled":             counts.Scheduled,
			"cancelled":             counts.Cancelled,
			"pending_oldest_age_sec": pendingAge,
			"total_workers":         workers,
			"queue_depth":           depth,
		})
	}
}

// redisSignals counts live worker heartbeats and total queued stream entries.
// Any Redis failure yields zeros with no error: stats must survive a
// delivery-layer blip.
func redisSignals(ctx context.Context, rds *broker.RedisStore) (workers int64, depth int64) {
	if rds == nil || rds.Conn == nil {
		return 0, 0
	}
	var cursor uint64
	for {
		keys, next, err := rds.Conn.Scan(ctx, cursor, "heartbeat:workers:*", 100).Result()
		if err != nil {
			return 0, 0
		}
		workers += int64(len(keys))
		if next == 0 {
			break
		}
		cursor = next
	}
	for _, stream := range watchedStreams {
		n, err := rds.Conn.XLen(ctx, stream).Result()
		if err != nil {
			continue
		}
		depth += n
	}
	return workers, depth
}
