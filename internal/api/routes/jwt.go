package routes

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Claims carries the user identity. Keep minimal: sub=user_id.
type Claims struct {
	jwt.RegisteredClaims
}

// IssueToken mints an HS256 JWT for userID.
func IssueToken(userID uuid.UUID, secret string, ttl time.Duration) (token string, expiresAt time.Time, err error) {
	if secret == "" {
		return "", time.Time{}, fmt.Errorf("auth: empty jwt secret")
	}
	expiresAt = time.Now().Add(ttl)
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	})
	signed, err := t.SignedString([]byte(secret))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, expiresAt, nil
}

// ParseToken validates an HS256 JWT and returns the user_id.
func ParseToken(raw, secret string) (uuid.UUID, error) {
	if secret == "" {
		return uuid.Nil, fmt.Errorf("auth: empty jwt secret")
	}
	t, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("auth: unexpected signing method")
		}
		return []byte(secret), nil
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("auth: invalid token: %w", err)
	}
	claims, ok := t.Claims.(*Claims)
	if !ok || !t.Valid {
		return uuid.Nil, fmt.Errorf("auth: invalid token")
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil, fmt.Errorf("auth: bad subject: %w", err)
	}
	return id, nil
}
