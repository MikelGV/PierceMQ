package middleware

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/MikelGV/PierceMQ/internal/auth"
	storageauth "github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// RequireAuth accepts either a JWT (human login) or an active API key
// (service auth). JWT is tried first; revoked/unknown keys yield 401.
// On success the user_id is attached to the request context.
func RequireAuth(userStore *users.UsersStore, keyStore *storageauth.Store, jwtSecret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := auth.BearerToken(r)
		if raw == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
			return
		}
		if id, err := auth.ParseToken(raw, jwtSecret); err == nil {
			next.ServeHTTP(w, r.WithContext(auth.WithUserID(r.Context(), id)))
			return
		}
		if keyStore != nil {
			if rec, err := keyStore.LookupActive(r.Context(), raw); err == nil {
				next.ServeHTTP(w, r.WithContext(auth.WithUserID(r.Context(), rec.UserID)))
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
