package usecase

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
	"golang.org/x/crypto/bcrypt"
)

// LoginRequest represents a login attempt.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse contains the authentication token.
type LoginResponse struct {
	Token     string    `json:"token"`
	UserID    uuid.UUID `json:"user_id"`
	StaffID   uuid.UUID `json:"staff_id"`
	Role      string    `json:"role"`
	ExpiresAt time.Time `json:"expires_at"`
}

// AuthUseCase orchestrates authentication and user management.
type AuthUseCase struct {
	userRepo  port.UserRepository
	jwtSecret string
}

func NewAuthUseCase(userRepo port.UserRepository, jwtSecret string) *AuthUseCase {
	return &AuthUseCase{userRepo: userRepo, jwtSecret: jwtSecret}
}

// Login authenticates a user and returns a token.
func (uc *AuthUseCase) Login(ctx context.Context, req LoginRequest) (*LoginResponse, error) {
	user, err := uc.userRepo.GetByUsername(ctx, req.Username)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}
	if user.IsDisabled() {
		return nil, fmt.Errorf("account is disabled")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	expiresAt := time.Now().Add(24 * time.Hour)
	token := uc.generateToken(user.ID, expiresAt)

	user.RecordLogin()
	_ = uc.userRepo.Update(ctx, user)

	return &LoginResponse{
		Token:     token,
		UserID:    user.ID,
		StaffID:   user.StaffID,
		Role:      user.Role,
		ExpiresAt: expiresAt,
	}, nil
}

// CreateUser creates a new user account with hashed password.
func (uc *AuthUseCase) CreateUser(ctx context.Context, user *model.User, password string) error {
	if user.Username == "" || password == "" {
		return fmt.Errorf("username and password are required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}
	user.ID = uuid.New()
	user.PasswordHash = string(hash)
	user.Active = true
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	return uc.userRepo.Create(ctx, user)
}

// GetUser retrieves a user by ID.
func (uc *AuthUseCase) GetUser(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return uc.userRepo.GetByID(ctx, id)
}

func (uc *AuthUseCase) ListUsers(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.User], error) {
	filter.Normalize()
	return uc.userRepo.List(ctx, filter)
}

func (uc *AuthUseCase) generateToken(userID uuid.UUID, expiresAt time.Time) string {
	data := fmt.Sprintf("%s|%d", userID.String(), expiresAt.Unix())
	mac := hmac.New(sha256.New, []byte(uc.jwtSecret))
	mac.Write([]byte(data))
	sig := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("%s.%s", data, sig)
}
