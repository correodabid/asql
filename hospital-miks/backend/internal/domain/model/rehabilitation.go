package model

import (
	"time"

	"github.com/google/uuid"
)

// SessionStatus represents the status of a rehab/physio session.
type SessionStatus string

const (
	SessionStatusScheduled  SessionStatus = "SCHEDULED"
	SessionStatusInProgress SessionStatus = "IN_PROGRESS"
	SessionStatusCompleted  SessionStatus = "COMPLETED"
	SessionStatusCancelled  SessionStatus = "CANCELLED"
)

// TherapyType represents the type of therapy.
type TherapyType string

const (
	TherapyTypePhysiotherapy  TherapyType = "PHYSIOTHERAPY"
	TherapyTypeOccupational   TherapyType = "OCCUPATIONAL"
	TherapyTypeSpeech         TherapyType = "SPEECH"
	TherapyTypeCardiacRehab   TherapyType = "CARDIAC_REHAB"
	TherapyTypeNeurological   TherapyType = "NEUROLOGICAL"
	TherapyTypePostSurgical   TherapyType = "POST_SURGICAL"
	TherapyTypeMusculoskeletal TherapyType = "MUSCULOSKELETAL"
)

// RehabPlan represents a rehabilitation treatment plan.
type RehabPlan struct {
	ID          uuid.UUID   `json:"id"`
	PatientID   uuid.UUID   `json:"patient_id"`
	TherapistID uuid.UUID   `json:"therapist_id"`
	DoctorID    uuid.UUID   `json:"doctor_id"`
	Type        TherapyType `json:"type"`
	Diagnosis   string      `json:"diagnosis"`
	Goals       string      `json:"goals"`
	StartDate   time.Time   `json:"start_date"`
	EndDate     *time.Time  `json:"end_date,omitempty"`
	Sessions    int         `json:"total_sessions"`
	Completed   int         `json:"completed_sessions"`
	Active      bool        `json:"active"`
	Notes       string      `json:"notes,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// RehabSession represents a single rehabilitation/physiotherapy session.
type RehabSession struct {
	ID          uuid.UUID     `json:"id"`
	PlanID      uuid.UUID     `json:"plan_id"`
	TherapistID uuid.UUID     `json:"therapist_id"`
	PatientID   uuid.UUID     `json:"patient_id"`
	Status      SessionStatus `json:"status"`
	ScheduledAt time.Time     `json:"scheduled_at"`
	Duration    int           `json:"duration_minutes"`
	Room        string        `json:"room,omitempty"`
	Exercises   string        `json:"exercises,omitempty"`
	Progress    string        `json:"progress,omitempty"`
	PainLevel   *int          `json:"pain_level,omitempty"` // 0-10 scale
	Notes       string        `json:"notes,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}
