package model

import (
	"time"

	"github.com/google/uuid"
)

// User represents an application user for authentication.
type User struct {
	ID           uuid.UUID `json:"id"`
	StaffID      uuid.UUID `json:"staff_id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"` // "ADMIN", "DOCTOR", "NURSE", "PHARMACIST", "BILLING", "RECEPTIONIST"
	Active       bool      `json:"active"`
	LastLoginAt  *time.Time `json:"last_login_at,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ListFilter provides common pagination and filtering.
type ListFilter struct {
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
	Search   string `json:"search,omitempty"`
	SortBy   string `json:"sort_by,omitempty"`
	SortDir  string `json:"sort_dir,omitempty"` // "ASC" or "DESC"
}

// ListResult wraps a paginated list result.
type ListResult[T any] struct {
	Items      []T `json:"items"`
	Total      int `json:"total"`
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalPages int `json:"total_pages"`
}

// DefaultPageSize is the default number of items per page.
const DefaultPageSize = 20

// MaxPageSize is the maximum number of items per page.
const MaxPageSize = 100

// Normalize ensures valid pagination parameters.
func (f *ListFilter) Normalize() {
	if f.Page < 1 {
		f.Page = 1
	}
	if f.PageSize < 1 {
		f.PageSize = DefaultPageSize
	}
	if f.PageSize > MaxPageSize {
		f.PageSize = MaxPageSize
	}
	if f.SortDir != "DESC" {
		f.SortDir = "ASC"
	}
}

// Offset returns the SQL offset for the current page.
func (f ListFilter) Offset() int {
	return (f.Page - 1) * f.PageSize
}
