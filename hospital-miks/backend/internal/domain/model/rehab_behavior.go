package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewRehabPlan creates a new RehabPlan entity.
func NewRehabPlan(patientID, therapistID, doctorID uuid.UUID, therapyType TherapyType, diagnosis, goals string, totalSessions int) (*RehabPlan, error) {
	p := &RehabPlan{
		ID:          uuid.New(),
		PatientID:   patientID,
		TherapistID: therapistID,
		DoctorID:    doctorID,
		Type:        therapyType,
		Diagnosis:   diagnosis,
		Goals:       goals,
		StartDate:   time.Now(),
		Sessions:    totalSessions,
		Completed:   0,
		Active:      true,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// Validate enforces RehabPlan invariants.
func (p *RehabPlan) Validate() error {
	if p.PatientID == uuid.Nil || p.TherapistID == uuid.Nil {
		return fmt.Errorf("rehab plan: patient and therapist are required")
	}
	if p.Sessions <= 0 {
		return fmt.Errorf("rehab plan: must have at least one session")
	}
	return nil
}

// IncrementCompleted records one more completed session. Returns true if
// the plan is now fully completed.
func (p *RehabPlan) IncrementCompleted() bool {
	p.Completed++
	p.UpdatedAt = time.Now()
	if p.Completed >= p.Sessions {
		p.Active = false
		now := time.Now()
		p.EndDate = &now
		return true
	}
	return false
}

// NewRehabSession creates a new RehabSession entity.
func NewRehabSession(planID, therapistID, patientID uuid.UUID, scheduledAt time.Time, durationMin int, room string) (*RehabSession, error) {
	if durationMin <= 0 {
		durationMin = 45
	}
	s := &RehabSession{
		ID:          uuid.New(),
		PlanID:      planID,
		TherapistID: therapistID,
		PatientID:   patientID,
		Status:      SessionStatusScheduled,
		ScheduledAt: scheduledAt,
		Duration:    durationMin,
		Room:        room,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if s.PlanID == uuid.Nil || s.TherapistID == uuid.Nil {
		return nil, fmt.Errorf("rehab session: plan and therapist are required")
	}
	return s, nil
}

// Complete marks a session as completed with progress notes.
func (s *RehabSession) Complete(progress, exercises, notes string, painLevel *int) error {
	if s.Status == SessionStatusCompleted || s.Status == SessionStatusCancelled {
		return fmt.Errorf("rehab session: cannot complete from status %s", s.Status)
	}
	if painLevel != nil && (*painLevel < 0 || *painLevel > 10) {
		return fmt.Errorf("rehab session: pain level must be 0-10")
	}
	s.Status = SessionStatusCompleted
	s.Progress = progress
	s.Exercises = exercises
	s.Notes = notes
	s.PainLevel = painLevel
	s.UpdatedAt = time.Now()
	return nil
}

// Cancel cancels a session.
func (s *RehabSession) Cancel() error {
	if s.Status != SessionStatusScheduled {
		return fmt.Errorf("rehab session: only scheduled sessions can be cancelled")
	}
	s.Status = SessionStatusCancelled
	s.UpdatedAt = time.Now()
	return nil
}
