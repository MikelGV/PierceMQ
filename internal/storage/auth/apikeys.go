package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// APIKey mirrors migrations/000009. KeyHash is hex(SHA256(plaintext)); the
// plaintext is shown once at creation and never stored.
type APIKey struct {
	KeyID     uuid.UUID
	UserID    uuid.UUID
	Name      string
	RevokedAt sql.NullTime
	CreatedAt time.Time
}

// Store routes internally: writes go to write (primary), reads to read.
type Store struct {
	write *sql.DB
	read  *sql.DB
}

// New wires a Store from the lean storage.Stores handles.
func New(write, read *sql.DB) *Store {
	return &Store{write: write, read: read}
}

// HashKey returns hex(SHA256(plaintext)) for lookup/storage.
func HashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// CreateKey generates a random plaintext key, stores its hash, and returns
// both the plaintext (show once) and the record.
func (s *Store) CreateKey(ctx context.Context, userID uuid.UUID, name string) (plaintext string, rec APIKey, err error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", rec, fmt.Errorf("auth: rand: %w", err)
	}
	plaintext = "pmq_" + hex.EncodeToString(raw[:])
	hash := HashKey(plaintext)

	err = s.write.QueryRowContext(ctx,
		`INSERT INTO api_keys (user_id, key_hash, name)
		VALUES ($1, $2, $3)
		RETURNING key_id, user_id, name, revoked_at, created_at`,
		userID, hash, name,
	).Scan(&rec.KeyID, &rec.UserID, &rec.Name, &rec.RevokedAt, &rec.CreatedAt)
	if err != nil {
		return "", rec, fmt.Errorf("auth: insert api key: %w", err)
	}
	return plaintext, rec, nil
}

// LookupActive resolves a plaintext Bearer key to its (user, key) record.
// Revoked keys and unknown hashes yield sql.ErrNoRows.
func (s *Store) LookupActive(ctx context.Context, plaintext string) (APIKey, error) {
	var rec APIKey
	hash := HashKey(plaintext)
	err := s.read.QueryRowContext(ctx,
		`SELECT key_id, user_id, name, revoked_at, created_at
		FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, hash,
	).Scan(&rec.KeyID, &rec.UserID, &rec.Name, &rec.RevokedAt, &rec.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rec, sql.ErrNoRows
		}
		return rec, fmt.Errorf("auth: lookup api key: %w", err)
	}
	return rec, nil
}

// ListKeys returns a user's keys newest-first (hashes never leave the DB).
func (s *Store) ListKeys(ctx context.Context, userID uuid.UUID) ([]APIKey, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT key_id, user_id, name, revoked_at, created_at
		FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("auth: list keys: %w", err)
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.KeyID, &k.UserID, &k.Name, &k.RevokedAt, &k.CreatedAt); err != nil {
			return nil, fmt.Errorf("auth: scan key: %w", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("auth: rows keys: %w", err)
	}
	return out, nil
}

// RevokeKey marks a key revoked (idempotent for already-revoked rows).
func (s *Store) RevokeKey(ctx context.Context, userID, keyID uuid.UUID) error {
	res, err := s.write.ExecContext(ctx,
		`UPDATE api_keys SET revoked_at = now()
		WHERE key_id = $1 AND user_id = $2 AND revoked_at IS NULL`, keyID, userID)
	if err != nil {
		return fmt.Errorf("auth: revoke key: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("auth: revoke rows: %w", err)
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
