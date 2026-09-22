package routes

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJWTRoundTrip(t *testing.T) {
	id := uuid.New()
	token, _, err := IssueToken(id, "test-secret", time.Hour)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	got, err := ParseToken(token, "test-secret")
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if got != id {
		t.Fatalf("ParseToken = %v, want %v", got, id)
	}
	if _, err := ParseToken(token, "wrong-secret"); err == nil {
		t.Fatal("ParseToken with wrong secret should fail")
	}
	if _, _, err := IssueToken(id, "", time.Hour); err == nil {
		t.Fatal("IssueToken with empty secret should fail")
	}
}
