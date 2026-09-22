package routes

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/MikelGV/PierceMQ/internal/storage/users"
	"golang.org/x/crypto/bcrypt"
)

type registerRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// NewRegisterHandler creates users with a bcrypt hash. Duplicate email -> 409.
func NewRegisterHandler(store *users.UsersStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req registerRequest
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.Email = strings.TrimSpace(req.Email)
		if req.Name == "" || req.Email == "" || len(req.Password) < 8 || !strings.Contains(req.Email, "@") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name, valid email and password (min 8 chars) are required"})
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "password hashing failed"})
			return
		}
		u, err := store.CreateUser(r.Context(), req.Name, req.Email, string(hash))
		if err != nil {
			if errors.Is(err, users.ErrUserExists) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "email already taken"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "create user failed"})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"user_id": u.UserID.String(), "email": u.Email})
	}
}

// NewLoginHandler verifies credentials and issues a JWT. Wrong creds -> 401
// with a generic message (no user enumeration).
func NewLoginHandler(store *users.UsersStore, jwtSecret string, ttl time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req loginRequest
		if err := decodeJSON(r, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
			return
		}
		u, err := store.GetUserByEmail(r.Context(), strings.TrimSpace(req.Email))
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "login failed"})
			return
		}
		if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
			return
		}
		token, expiresAt, err := IssueToken(u.UserID, jwtSecret, ttl)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "token issue failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"token": token, "expires_at": expiresAt.UTC().Format(time.RFC3339)})
	}
}
