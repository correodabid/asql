package http

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	ctxUserID contextKey = "userID"
)

// UserIDFromContext extracts the authenticated user ID from the request context.
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	v, ok := ctx.Value(ctxUserID).(uuid.UUID)
	return v, ok
}

// AuthMiddleware validates Bearer tokens in the Authorization header.
// Token format: "userID|expiresAtUnix.hmacSignature"
func AuthMiddleware(jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if header == "" {
				respondError(w, http.StatusUnauthorized, "missing authorization header")
				return
			}

			token := strings.TrimPrefix(header, "Bearer ")
			if token == header {
				respondError(w, http.StatusUnauthorized, "invalid authorization format")
				return
			}

			userID, err := validateToken(token, jwtSecret)
			if err != nil {
				respondError(w, http.StatusUnauthorized, err.Error())
				return
			}

			ctx := context.WithValue(r.Context(), ctxUserID, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// validateToken parses and verifies the HMAC-signed token.
func validateToken(token, secret string) (uuid.UUID, error) {
	// Split "data.signature" where data = "userID|expiresUnix"
	dotIdx := strings.LastIndex(token, ".")
	if dotIdx < 0 {
		return uuid.Nil, fmt.Errorf("malformed token")
	}
	data := token[:dotIdx]
	sig := token[dotIdx+1:]

	// Verify HMAC
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return uuid.Nil, fmt.Errorf("invalid token signature")
	}

	// Parse "userID|expiresUnix"
	parts := strings.SplitN(data, "|", 2)
	if len(parts) != 2 {
		return uuid.Nil, fmt.Errorf("malformed token payload")
	}

	userID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid user ID in token")
	}

	expiresUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid expiration in token")
	}

	if time.Now().Unix() > expiresUnix {
		return uuid.Nil, fmt.Errorf("token expired")
	}

	return userID, nil
}
