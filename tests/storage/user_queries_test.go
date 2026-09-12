package storage_test

import "testing"

func CreateUser_Test(t *testing.T) {
	t.Run("user gets created successfully", func(t *testing.T) {})
	t.Skip("user creation failure due to email is already used in a different account")
	t.Skip("user creation failure due to password is too short")
	t.Skip("user creation failure due to network issues")
}
