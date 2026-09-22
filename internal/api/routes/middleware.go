package routes

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
	"github.com/google/uuid"
)

type ctxKey string

const userIDKey ctxKey = "piercemq_user_id"

// UserIDFromContext returns the authenticated user_id set by RequireAuth.
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(userIDKey).(uuid.UUID)
	return id, ok
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	rest, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || rest == "" {
		return ""
	}
	return strings.TrimSpace(rest)
}

// RequireAuth accepts either a JWT (human login) or an active API key
// (service auth). JWT is tried first; revoked/unknown keys yield 401.
// On success the user_id is attached to the request context.
func RequireAuth(userStore *users.UsersStore, keyStore *auth.Store, jwtSecret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := bearerToken(r)
		if raw == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
			return
		}
		if id, err := ParseToken(raw, jwtSecret); err == nil {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDKey, id)))
			return
		}
		if keyStore != nil {
			if rec, err := keyStore.LookupActive(r.Context(), raw); err == nil {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDKey, rec.UserID)))
				return
			} else if !errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "auth lookup failed"})
				return
			}
		}
		_ = userStore
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or expired credentials"})
	})
}
