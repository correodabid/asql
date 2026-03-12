package model

import (
	"time"
)

// RecordLogin updates the last login timestamp for a User.
func (u *User) RecordLogin() {
	now := time.Now()
	u.LastLoginAt = &now
	u.UpdatedAt = now
}

// IsDisabled returns true if the user account is disabled.
func (u *User) IsDisabled() bool {
	return !u.Active
}
