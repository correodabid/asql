package model

import (
	"time"

	"github.com/google/uuid"
)

// GuardShiftType represents the type of guard shift.
type GuardShiftType string

const (
	GuardShiftMorning   GuardShiftType = "MORNING"
	GuardShiftAfternoon GuardShiftType = "AFTERNOON"
	GuardShiftNight     GuardShiftType = "NIGHT"
	GuardShift24H       GuardShiftType = "24H"
	GuardShiftWeekend   GuardShiftType = "WEEKEND"
)

// GuardShiftStatus represents the status of a guard shift.
type GuardShiftStatus string

const (
	GuardShiftStatusScheduled GuardShiftStatus = "SCHEDULED"
	GuardShiftStatusActive    GuardShiftStatus = "ACTIVE"
	GuardShiftStatusCompleted GuardShiftStatus = "COMPLETED"
	GuardShiftStatusSwapped   GuardShiftStatus = "SWAPPED"
	GuardShiftStatusCancelled GuardShiftStatus = "CANCELLED"
)

// GuardShift represents a guard duty assignment.
type GuardShift struct {
	ID           uuid.UUID        `json:"id"`
	StaffID      uuid.UUID        `json:"staff_id"`
	DepartmentID uuid.UUID        `json:"department_id"`
	Type         GuardShiftType   `json:"type"`
	Status       GuardShiftStatus `json:"status"`
	StartTime    time.Time        `json:"start_time"`
	EndTime      time.Time        `json:"end_time"`
	Notes        string           `json:"notes,omitempty"`
	SwappedWith  *uuid.UUID       `json:"swapped_with,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}
