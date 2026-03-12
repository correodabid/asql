package model

import (
	"time"

	"github.com/google/uuid"
)

// AdmissionStatus represents the status of a hospital admission.
type AdmissionStatus string

const (
	AdmissionStatusAdmitted   AdmissionStatus = "ADMITTED"
	AdmissionStatusInCare     AdmissionStatus = "IN_CARE"
	AdmissionStatusDischarged AdmissionStatus = "DISCHARGED"
	AdmissionStatusTransfer   AdmissionStatus = "TRANSFERRED"
)

// BedStatus represents the status of a hospital bed.
type BedStatus string

const (
	BedStatusAvailable BedStatus = "AVAILABLE"
	BedStatusOccupied  BedStatus = "OCCUPIED"
	BedStatusReserved  BedStatus = "RESERVED"
	BedStatusCleaning  BedStatus = "CLEANING"
	BedStatusOutOfOrder BedStatus = "OUT_OF_ORDER"
)

// MealType represents a meal type.
type MealType string

const (
	MealTypeBreakfast MealType = "BREAKFAST"
	MealTypeLunch     MealType = "LUNCH"
	MealTypeDinner    MealType = "DINNER"
	MealTypeSnack     MealType = "SNACK"
)

// Ward represents a hospital ward.
type Ward struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Code         string    `json:"code"`
	DepartmentID uuid.UUID `json:"department_id"`
	Floor        int       `json:"floor"`
	Building     string    `json:"building"`
	TotalBeds    int       `json:"total_beds"`
	Active       bool      `json:"active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Bed represents a hospital bed.
type Bed struct {
	ID        uuid.UUID `json:"id"`
	WardID    uuid.UUID `json:"ward_id"`
	Number    string    `json:"number"`
	Status    BedStatus `json:"status"`
	RoomNo    string    `json:"room_no"`
	Features  string    `json:"features,omitempty"` // e.g. "OXYGEN,MONITORING,ADJUSTABLE"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Admission represents a patient hospital admission.
type Admission struct {
	ID              uuid.UUID       `json:"id"`
	PatientID       uuid.UUID       `json:"patient_id"`
	AdmittingDocID  uuid.UUID       `json:"admitting_doctor_id"`
	BedID           uuid.UUID       `json:"bed_id"`
	DepartmentID    uuid.UUID       `json:"department_id"`
	Status          AdmissionStatus `json:"status"`
	AdmissionDate   time.Time       `json:"admission_date"`
	DischargeDate   *time.Time      `json:"discharge_date,omitempty"`
	Diagnosis       string          `json:"diagnosis"`
	AdmissionReason string          `json:"admission_reason"`
	DietaryNeeds    string          `json:"dietary_needs,omitempty"`
	Notes           string          `json:"notes,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// MealOrder represents a meal order for an admitted patient.
type MealOrder struct {
	ID          uuid.UUID `json:"id"`
	AdmissionID uuid.UUID `json:"admission_id"`
	MealType    MealType  `json:"meal_type"`
	Date        time.Time `json:"date"`
	Menu        string    `json:"menu"`
	DietaryNote string    `json:"dietary_note,omitempty"`
	Delivered   bool      `json:"delivered"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// CareNote represents a nursing care note for an admission.
type CareNote struct {
	ID          uuid.UUID `json:"id"`
	AdmissionID uuid.UUID `json:"admission_id"`
	StaffID     uuid.UUID `json:"staff_id"`
	NoteType    string    `json:"note_type"` // "VITALS", "MEDICATION", "OBSERVATION", "PROCEDURE"
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"created_at"`
}
