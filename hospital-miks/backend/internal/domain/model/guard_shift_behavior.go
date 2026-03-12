package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewGuardShift creates a new GuardShift entity with validation.
func NewGuardShift(staffID, departmentID uuid.UUID, shiftType GuardShiftType, start, end time.Time) (*GuardShift, error) {
	gs := &GuardShift{
		ID:           uuid.New(),
		StaffID:      staffID,
		DepartmentID: departmentID,
		Type:         shiftType,
		Status:       GuardShiftStatusScheduled,
		StartTime:    start,
		EndTime:      end,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if err := gs.Validate(); err != nil {
		return nil, err
	}
	return gs, nil
}

// Validate enforces GuardShift invariants.
func (gs *GuardShift) Validate() error {
	if gs.StaffID == uuid.Nil || gs.DepartmentID == uuid.Nil {
		return fmt.Errorf("guard shift: staff and department are required")
	}
	if gs.EndTime.Before(gs.StartTime) {
		return fmt.Errorf("guard shift: end time must be after start time")
	}
	return nil
}

// Swap transfers the shift to another staff member.
func (gs *GuardShift) Swap(newStaffID uuid.UUID) error {
	if gs.Status != GuardShiftStatusScheduled {
		return fmt.Errorf("guard shift: only scheduled shifts can be swapped")
	}
	gs.SwappedWith = &newStaffID
	gs.Status = GuardShiftStatusSwapped
	gs.UpdatedAt = time.Now()
	return nil
}
