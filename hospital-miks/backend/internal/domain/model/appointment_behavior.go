package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewAppointment creates a new Appointment entity with validation.
func NewAppointment(patientID, doctorID, departmentID uuid.UUID, apptType AppointmentType, scheduledAt time.Time, durationMin int) (*Appointment, error) {
	if durationMin <= 0 {
		durationMin = 30
	}
	a := &Appointment{
		ID:           uuid.New(),
		PatientID:    patientID,
		DoctorID:     doctorID,
		DepartmentID: departmentID,
		Type:         apptType,
		Status:       AppointmentStatusScheduled,
		ScheduledAt:  scheduledAt,
		Duration:     durationMin,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return a, nil
}

// Validate enforces Appointment domain invariants.
func (a *Appointment) Validate() error {
	if a.PatientID == uuid.Nil || a.DoctorID == uuid.Nil {
		return fmt.Errorf("appointment: patient and doctor are required")
	}
	if a.ScheduledAt.Before(time.Now().Add(-1 * time.Minute)) {
		return fmt.Errorf("appointment: cannot be scheduled in the past")
	}
	return nil
}

// EndTime returns the computed end time.
func (a *Appointment) EndTime() time.Time {
	return a.ScheduledAt.Add(time.Duration(a.Duration) * time.Minute)
}

// ConflictsWith checks whether two appointments overlap in time.
func (a *Appointment) ConflictsWith(other Appointment) bool {
	if other.Status == AppointmentStatusCancelled {
		return false
	}
	return a.ScheduledAt.Before(other.EndTime()) && a.EndTime().After(other.ScheduledAt)
}

// Confirm transitions a SCHEDULED appointment to CONFIRMED.
func (a *Appointment) Confirm() error {
	if a.Status != AppointmentStatusScheduled {
		return fmt.Errorf("appointment: cannot confirm from status %s", a.Status)
	}
	a.Status = AppointmentStatusConfirmed
	a.UpdatedAt = time.Now()
	return nil
}

// Cancel cancels the appointment.
func (a *Appointment) Cancel() error {
	if a.Status == AppointmentStatusCompleted || a.Status == AppointmentStatusCancelled {
		return fmt.Errorf("appointment: cannot cancel from status %s", a.Status)
	}
	a.Status = AppointmentStatusCancelled
	a.UpdatedAt = time.Now()
	return nil
}

// Complete marks the appointment as completed with diagnosis and notes.
func (a *Appointment) Complete(diagnosis, notes string) error {
	if a.Status == AppointmentStatusCancelled || a.Status == AppointmentStatusCompleted {
		return fmt.Errorf("appointment: cannot complete from status %s", a.Status)
	}
	a.Status = AppointmentStatusCompleted
	a.Diagnosis = diagnosis
	a.Notes = notes
	a.UpdatedAt = time.Now()
	return nil
}
