package auth_test

import (
	"testing"
	"time"

	"github.com/MikelGV/PierceMQ/internal/auth"
	"github.com/google/uuid"
)

func TestJWTRoundTrip(t *testing.T) {
	id := uuid.New()
	token, _, err := auth.IssueToken(id, "test-secret", time.Hour)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	got, err := auth.ParseToken(token, "test-secret")
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if got != id {
		t.Fatalf("ParseToken = %v, want %v", got, id)
	}
	if _, err := auth.ParseToken(token, "wrong-secret"); err == nil {
		t.Fatal("ParseToken with wrong secret should fail")
	}
	if _, _, err := auth.IssueToken(id, "", time.Hour); err == nil {
		t.Fatal("IssueToken with empty secret should fail")
	}
}

func TestPasswordRoundTrip(t *testing.T) {
	hash, err := auth.HashPassword("supersecret")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if err := auth.VerifyPassword(hash, "supersecret"); err != nil {
		t.Fatalf("VerifyPassword correct: %v", err)
	}
	if err := auth.VerifyPassword(hash, "wrong"); err == nil {
		t.Fatal("VerifyPassword wrong should fail")
	}
}
