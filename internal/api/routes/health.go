package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/storage"
)

type healthStatus struct {
	Status string `json:"status"`
	Redis  string `json:"redis"`
	DBWrite string `json:"db_write"`
	DBRead  string `json:"db_read"`
}

// NewHealthHandler deep-checks Redis + both DB pools. Ping failures yield
// 503 with per-dependency status; success yields 200 {"status":"ok",...}.
// GET-only; PgBouncer-safe (plain Ping, no session state).
func NewHealthHandler(rds *broker.RedisStore, stores *storage.Stores) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		status := healthStatus{Redis: "up", DBWrite: "up", DBRead: "up", Status: "ok"}
		code := http.StatusOK

		fail := func() { status.Status = "unavailable"; code = http.StatusServiceUnavailable }

		if rds == nil || rds.Conn == nil {
			status.Redis = "missing"
			fail()
		} else if err := rds.Conn.Ping(ctx).Err(); err != nil {
			status.Redis = "down: " + err.Error()
			fail()
		}

		if stores == nil || stores.Write == nil || stores.Write.Conn == nil {
			status.DBWrite = "missing"
			fail()
		} else if err := stores.Write.Conn.PingContext(ctx); err != nil {
			status.DBWrite = "down: " + err.Error()
			fail()
		}

		if stores == nil || stores.Read == nil || stores.Read.Conn == nil {
			status.DBRead = "missing"
			fail()
		} else if err := stores.Read.Conn.PingContext(ctx); err != nil {
			status.DBRead = "down: " + err.Error()
			fail()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(status)
	}
}
