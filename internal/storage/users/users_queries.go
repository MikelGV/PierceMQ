package users

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// UsersStore routes internally: writes go to write (primary via PgBouncer
// `piercemq`), plain reads go to read (replicas via `piercemq_ro`).
// Callers never pick a handle.
type UsersStore struct {
	write *sql.DB
	read  *sql.DB
}

// User mirrors migrations/000008. PasswordHash is bcrypt output; plaintext
// passwords never reach this package.
type User struct {
	UserID       uuid.UUID
	Name         string
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

// ErrUserExists is returned when email is already taken. Detect via errors.Is.
var ErrUserExists = errors.New("users: email already taken")

// New wires a UsersStore from the lean storage.Stores handles.
func New(write, read *sql.DB) *UsersStore {
	return &UsersStore{write: write, read: read}
}

func scanUser(u *User, row interface {
	Scan(dest ...any) error
}) error {
	return row.Scan(&u.UserID, &u.Name, &u.Email, &u.PasswordHash, &u.CreatedAt)
}

const userColumns = `user_id, user_name, email, password_hash, created_at`

// CreateUser inserts a user. passwordHash must be a bcrypt hash computed by
// the caller. Duplicate email yields ErrUserExists.
func (u *UsersStore) CreateUser(ctx context.Context, name, email, passwordHash string) (User, error) {
	var out User
	if name == "" || email == "" || passwordHash == "" {
		return out, fmt.Errorf("users: name, email and password hash are required")
	}
	err := scanUser(&out, u.write.QueryRowContext(ctx,
		`INSERT INTO users (user_name, email, password_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (email) DO NOTHING
		RETURNING `+userColumns, name, email, passwordHash))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, ErrUserExists
		}
		return out, fmt.Errorf("insert user: %w", err)
	}
	return out, nil
}

// GetUserByEmail fetches a user for login (includes password hash).
func (u *UsersStore) GetUserByEmail(ctx context.Context, email string) (User, error) {
	var out User
	if err := scanUser(&out, u.read.QueryRowContext(ctx,
		`SELECT `+userColumns+` FROM users WHERE email = $1`, email)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, sql.ErrNoRows
		}
		return out, fmt.Errorf("get user by email: %w", err)
	}
	return out, nil
}

// GetUserByID fetches a user profile (includes password hash; callers strip).
func (u *UsersStore) GetUserByID(ctx context.Context, id uuid.UUID) (User, error) {
	var out User
	if err := scanUser(&out, u.read.QueryRowContext(ctx,
		`SELECT `+userColumns+` FROM users WHERE user_id = $1`, id)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, sql.ErrNoRows
		}
		return out, fmt.Errorf("get user by id: %w", err)
	}
	return out, nil
}
