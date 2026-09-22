package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type ctxKey string

const userIDKey ctxKey = "piercemq_user_id"

// UserIDFromContext returns the authenticated user_id set by RequireAuth.
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(userIDKey).(uuid.UUID)
	return id, ok
}

// WithUserID attaches the authenticated user_id to the request context.
func WithUserID(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

// BearerToken extracts the bearer token from the Authorization header.
func BearerToken(r *http.Request) string {
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
