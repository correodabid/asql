package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewOperatingRoom creates a new OperatingRoom entity.
func NewOperatingRoom(name, code, building string, floor, capacity int) (*OperatingRoom, error) {
	or := &OperatingRoom{
		ID:        uuid.New(),
		Name:      name,
		Code:      code,
		Floor:     floor,
		Building:  building,
		Status:    ORStatusAvailable,
		Capacity:  capacity,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := or.Validate(); err != nil {
		return nil, err
	}
	return or, nil
}

// Validate enforces OperatingRoom invariants.
func (or *OperatingRoom) Validate() error {
	if or.Name == "" || or.Code == "" {
		return fmt.Errorf("operating room: name and code are required")
	}
	return nil
}

// NewSurgery creates a new Surgery entity with validation.
func NewSurgery(patientID, leadSurgeonID, anesthetistID, operatingRoomID uuid.UUID, procName, procCode string, start, end time.Time) (*Surgery, error) {
	s := &Surgery{
		ID:              uuid.New(),
		PatientID:       patientID,
		LeadSurgeonID:   leadSurgeonID,
		AnesthetistID:   anesthetistID,
		OperatingRoomID: operatingRoomID,
		ProcedureName:   procName,
		ProcedureCode:   procCode,
		Status:          SurgeryStatusScheduled,
		ScheduledStart:  start,
		ScheduledEnd:    end,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Validate enforces Surgery invariants.
func (s *Surgery) Validate() error {
	if s.PatientID == uuid.Nil || s.LeadSurgeonID == uuid.Nil {
		return fmt.Errorf("surgery: patient and lead surgeon are required")
	}
	if s.OperatingRoomID == uuid.Nil {
		return fmt.Errorf("surgery: operating room is required")
	}
	if s.ScheduledStart.After(s.ScheduledEnd) {
		return fmt.Errorf("surgery: start must be before end")
	}
	return nil
}

// ConflictsWith checks whether two surgeries overlap in time for the same OR.
func (s *Surgery) ConflictsWith(other Surgery) bool {
	if other.Status == SurgeryStatusCancelled {
		return false
	}
	return s.ScheduledStart.Before(other.ScheduledEnd) && s.ScheduledEnd.After(other.ScheduledStart)
}

// Start moves the surgery to IN_PROGRESS.
func (s *Surgery) Start() error {
	if s.Status != SurgeryStatusScheduled && s.Status != SurgeryStatusPreOp {
		return fmt.Errorf("surgery: cannot start from status %s", s.Status)
	}
	now := time.Now()
	s.Status = SurgeryStatusInProgress
	s.ActualStart = &now
	s.UpdatedAt = now
	return nil
}

// Complete marks the surgery as COMPLETED.
func (s *Surgery) Complete(postOpNotes, complications string) error {
	if s.Status != SurgeryStatusInProgress {
		return fmt.Errorf("surgery: cannot complete from status %s", s.Status)
	}
	now := time.Now()
	s.Status = SurgeryStatusCompleted
	s.ActualEnd = &now
	s.PostOpNotes = postOpNotes
	s.Complications = complications
	s.UpdatedAt = now
	return nil
}

// Cancel cancels the surgery.
func (s *Surgery) Cancel() error {
	if s.Status == SurgeryStatusCompleted || s.Status == SurgeryStatusCancelled {
		return fmt.Errorf("surgery: cannot cancel from status %s", s.Status)
	}
	s.Status = SurgeryStatusCancelled
	s.UpdatedAt = time.Now()
	return nil
}
