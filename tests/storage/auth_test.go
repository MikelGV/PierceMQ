package storage_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/MikelGV/PierceMQ/internal/storage"
	"github.com/MikelGV/PierceMQ/internal/storage/auth"
	"github.com/MikelGV/PierceMQ/internal/storage/users"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func setupAuthStores(t *testing.T) (*users.UsersStore, *auth.Store) {
	t.Helper()
	ctx := context.Background()
	dsn := setupPostgres(t)
	require.NoError(t, storage.MigrateUp(ctx, dsn))

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return users.New(db, db), auth.New(db, db)
}

func TestUsersStore(t *testing.T) {
	ctx := context.Background()

	t.Run("create ok then duplicate email is ErrUserExists", func(t *testing.T) {
		us, _ := setupAuthStores(t)

		hash, err := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.MinCost)
		require.NoError(t, err)

		u, err := us.CreateUser(ctx, "Ada", "ada@example.com", string(hash))
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, u.UserID)
		require.Equal(t, "ada@example.com", u.Email)

		_, err = us.CreateUser(ctx, "Ada", "ada@example.com", string(hash))
		require.ErrorIs(t, err, users.ErrUserExists)
	})

	t.Run("getters round-trip, missing is sql.ErrNoRows", func(t *testing.T) {
		us, _ := setupAuthStores(t)

		hash, err := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.MinCost)
		require.NoError(t, err)

		created, err := us.CreateUser(ctx, "Grace", "grace@example.com", string(hash))
		require.NoError(t, err)

		byEmail, err := us.GetUserByEmail(ctx, "grace@example.com")
		require.NoError(t, err)
		require.Equal(t, created.UserID, byEmail.UserID)
		require.NoError(t, bcrypt.CompareHashAndPassword(
			[]byte(byEmail.PasswordHash), []byte("password123")))

		byID, err := us.GetUserByID(ctx, created.UserID)
		require.NoError(t, err)
		require.Equal(t, "grace@example.com", byID.Email)

		_, err = us.GetUserByEmail(ctx, "nobody@example.com")
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestAPIKeysStore(t *testing.T) {
	ctx := context.Background()

	t.Run("create, lookup, list, revoke", func(t *testing.T) {
		us, ks := setupAuthStores(t)

		hash, err := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.MinCost)
		require.NoError(t, err)
		u, err := us.CreateUser(ctx, "Svc", "svc@example.com", string(hash))
		require.NoError(t, err)

		plaintext, rec, err := ks.CreateKey(ctx, u.UserID, "worker-1")
		require.NoError(t, err)
		require.NotEmpty(t, plaintext)
		require.NotEqual(t, uuid.Nil, rec.KeyID)
		require.Equal(t, u.UserID, rec.UserID)

		found, err := ks.LookupActive(ctx, plaintext)
		require.NoError(t, err)
		require.Equal(t, rec.KeyID, found.KeyID)

		_, err = ks.LookupActive(ctx, "pmq_bogus")
		require.ErrorIs(t, err, sql.ErrNoRows)

		listed, err := ks.ListKeys(ctx, u.UserID)
		require.NoError(t, err)
		require.Len(t, listed, 1)

		require.NoError(t, ks.RevokeKey(ctx, u.UserID, rec.KeyID))
		_, err = ks.LookupActive(ctx, plaintext)
		require.ErrorIs(t, err, sql.ErrNoRows, "revoked keys must not authenticate")

		// Revoking twice targets zero active rows.
		require.ErrorIs(t, ks.RevokeKey(ctx, u.UserID, rec.KeyID), sql.ErrNoRows)
	})
}
