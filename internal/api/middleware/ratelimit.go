package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/MikelGV/PierceMQ/internal/config"
)

// RateLimiter is a per-caller token-bucket limiter (§7.6, §12.5: a burst
// from one client must not deplete write capacity for everyone else).
// Callers are keyed by Authorization header (distinct users/keys get
// distinct buckets) falling back to client IP for unauthenticated hits.
// Limits are read from cfg on every request so tests can tune them live;
// non-positive values fall back to the package defaults.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	cfg     *config.Config
}

type bucket struct {
	tokens float64
	last   time.Time
}

// exemptPaths bypass rate limiting: liveness probes must never 429.
var exemptPaths = map[string]bool{
	"/health":  true,
	"/healthz": true,
}

func NewRateLimiter(cfg *config.Config) *RateLimiter {
	return &RateLimiter{buckets: make(map[string]*bucket), cfg: cfg}
}

func (l *RateLimiter) limits() (rps float64, burst float64) {
	rps, burst = float64(config.DefaultRateLimitRPS), float64(config.DefaultRateLimitBurst)
	if l.cfg != nil {
		if l.cfg.RateLimitRPS > 0 {
			rps = float64(l.cfg.RateLimitRPS)
		}
		if l.cfg.RateLimitBurst > 0 {
			burst = float64(l.cfg.RateLimitBurst)
		}
	}
	return rps, burst
}

func callerKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		return "auth:" + h
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "ip:" + r.RemoteAddr
	}
	return "ip:" + host
}

// Allow reports whether a request from key may proceed.
func (l *RateLimiter) Allow(key string) bool {
	rps, burst := l.limits()
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: burst, last: now}
		l.buckets[key] = b
		// Bound memory: drop all buckets if the map grows pathological.
		// Buckets rebuild lazily at full burst, a safe direction.
		if len(l.buckets) > 100000 {
			l.buckets = map[string]*bucket{key: b}
		}
	}
	b.tokens += now.Sub(b.last).Seconds() * rps
	if b.tokens > burst {
		b.tokens = burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimit wraps next, rejecting over-limit callers with 429 +
// Retry-After. Health probes are exempt.
func RateLimit(cfg *config.Config, next http.Handler) http.Handler {
	l := NewRateLimiter(cfg)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !exemptPaths[r.URL.Path] && !l.Allow(callerKey(r)) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
