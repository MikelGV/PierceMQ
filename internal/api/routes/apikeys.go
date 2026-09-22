package routes

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/google/uuid"
)

// NewAPIKeysHandler serves POST /v1/keys (create, plaintext shown once) and
// GET /v1/keys (list, hashes never leave the DB). Caller must wrap with
// RequireAuth; key-creation via API key is allowed (services can rotate).
func NewAPIKeysHandler(store *auth.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, ok := UserIDFromContext(r.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Name string `json:"name"`
			}
			if err := decodeJSON(r, &req); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
				return
			}
			plaintext, rec, err := store.CreateKey(r.Context(), userID, strings.TrimSpace(req.Name))
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "create key failed"})
				return
			}
			writeJSON(w, http.StatusCreated, map[string]string{
				"key":     plaintext,
				"key_id":  rec.KeyID.String(),
				"warning": "store this key now; it is never shown again",
			})
		case http.MethodGet:
			keys, err := store.ListKeys(r.Context(), userID)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "list keys failed"})
				return
			}
			type keyOut struct {
				KeyID     string  `json:"key_id"`
				Name      string  `json:"name"`
				RevokedAt *string `json:"revoked_at,omitempty"`
				CreatedAt string  `json:"created_at"`
			}
			out := make([]keyOut, 0, len(keys))
			for _, k := range keys {
				o := keyOut{KeyID: k.KeyID.String(), Name: k.Name, CreatedAt: k.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00")}
				if k.RevokedAt.Valid {
					s := k.RevokedAt.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
					o.RevokedAt = &s
				}
				out = append(out, o)
			}
			writeJSON(w, http.StatusOK, map[string]any{"keys": out})
		default:
			w.Header().Set("Allow", "GET, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	}
}

// NewAPIKeyRevokeHandler serves POST /v1/keys/revoke {key_id}.
func NewAPIKeyRevokeHandler(store *auth.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		userID, ok := UserIDFromContext(r.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
			return
		}
		var req struct {
			KeyID string `json:"key_id"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
			return
		}
		keyID, err := uuid.Parse(req.KeyID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid key_id"})
			return
		}
		if err := store.RevokeKey(r.Context(), userID, keyID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "revoke failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
	}
}
