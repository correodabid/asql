package model

import (
	"time"

	"github.com/google/uuid"
)

// AppointmentStatus represents the status of an appointment.
type AppointmentStatus string

const (
	AppointmentStatusScheduled  AppointmentStatus = "SCHEDULED"
	AppointmentStatusConfirmed  AppointmentStatus = "CONFIRMED"
	AppointmentStatusInProgress AppointmentStatus = "IN_PROGRESS"
	AppointmentStatusCompleted  AppointmentStatus = "COMPLETED"
	AppointmentStatusCancelled  AppointmentStatus = "CANCELLED"
	AppointmentStatusNoShow     AppointmentStatus = "NO_SHOW"
)

// AppointmentType represents the type of appointment.
type AppointmentType string

const (
	AppointmentTypeConsultation  AppointmentType = "CONSULTATION"
	AppointmentTypeFollowUp      AppointmentType = "FOLLOW_UP"
	AppointmentTypeEmergency     AppointmentType = "EMERGENCY"
	AppointmentTypeSurgery       AppointmentType = "SURGERY"
	AppointmentTypeRehab         AppointmentType = "REHABILITATION"
	AppointmentTypePhysiotherapy AppointmentType = "PHYSIOTHERAPY"
	AppointmentTypeLab           AppointmentType = "LAB_TEST"
	AppointmentTypeImaging       AppointmentType = "IMAGING"
)

// Appointment represents a medical appointment or consultation.
type Appointment struct {
	ID           uuid.UUID         `json:"id"`
	PatientID    uuid.UUID         `json:"patient_id"`
	DoctorID     uuid.UUID         `json:"doctor_id"`
	DepartmentID uuid.UUID         `json:"department_id"`
	Type         AppointmentType   `json:"type"`
	Status       AppointmentStatus `json:"status"`
	ScheduledAt  time.Time         `json:"scheduled_at"`
	Duration     int               `json:"duration_minutes"`
	Room         string            `json:"room,omitempty"`
	Notes        string            `json:"notes,omitempty"`
	Diagnosis    string            `json:"diagnosis,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// ConsultationRoom represents a consultation room.
type ConsultationRoom struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Code         string    `json:"code"`
	DepartmentID uuid.UUID `json:"department_id"`
	Floor        int       `json:"floor"`
	Building     string    `json:"building"`
	Equipment    string    `json:"equipment,omitempty"`
	Active       bool      `json:"active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
