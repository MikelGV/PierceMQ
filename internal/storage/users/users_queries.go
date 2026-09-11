package users

import (
	"context"
	"database/sql"
	"fmt"
)

// UsersStore routes internally: writes (+ SELECT ... FOR UPDATE) go to
// write (primary via PgBouncer `piercemq`), plain reads go to read
// (replicas via `piercemq_ro`). Callers never pick a handle.
type UsersStore struct {
	write *sql.DB
	read  *sql.DB
}

type User struct {
	Name     string
	Email    string
	Password string
}

// New wires a UsersStore from the lean storage.Stores handles.
// Both must be non-nil; read may lag (replica offload).
func New(write, read *sql.DB) *UsersStore {
	return &UsersStore{write: write, read: read}
}

func (u *UsersStore) CreateUser(ctx context.Context, usr *User) error {
	trans, err := u.write.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("Error couldn't start transaction: %w", err)
	}

	defer trans.Rollback()
	query := `INSERT INTO users (
		user_name,
		email,
		password,
	) VALUES ($1, $2, $3)
	ON CONFLICT (email) WHERE idempontecy_key IS NOT NULL DO NOTHING
	RETURNING user_id, user_name, email, created_at;`

	err = trans.QueryRowContext(ctx, query, usr.Name, usr.Email, usr.Password).
		Scan(&usr.Name, &usr.Email, &usr.Password)

	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("User already exists: %w", err)
		}
		return fmt.Errorf("insert user: %w", err)
	}

	if err := trans.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil

}
