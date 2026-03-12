package model

import (
	"time"

	"github.com/google/uuid"
)

// Gender represents patient gender.
type Gender string

const (
	GenderMale    Gender = "MALE"
	GenderFemale  Gender = "FEMALE"
	GenderOther   Gender = "OTHER"
)

// BloodType represents a blood type.
type BloodType string

const (
	BloodTypeAPos  BloodType = "A+"
	BloodTypeANeg  BloodType = "A-"
	BloodTypeBPos  BloodType = "B+"
	BloodTypeBNeg  BloodType = "B-"
	BloodTypeABPos BloodType = "AB+"
	BloodTypeABNeg BloodType = "AB-"
	BloodTypeOPos  BloodType = "O+"
	BloodTypeONeg  BloodType = "O-"
)

// Patient represents a hospital patient.
type Patient struct {
	ID              uuid.UUID `json:"id"`
	MedicalRecordNo string    `json:"medical_record_no"`
	FirstName       string    `json:"first_name"`
	LastName        string    `json:"last_name"`
	DateOfBirth     time.Time `json:"date_of_birth"`
	Gender          Gender    `json:"gender"`
	NationalID      string    `json:"national_id"`
	Phone           string    `json:"phone"`
	Email           string    `json:"email,omitempty"`
	Address         string    `json:"address"`
	City            string    `json:"city"`
	PostalCode      string    `json:"postal_code"`
	BloodType       BloodType `json:"blood_type,omitempty"`
	Allergies       string    `json:"allergies,omitempty"`
	EmergencyName   string    `json:"emergency_contact_name"`
	EmergencyPhone  string    `json:"emergency_contact_phone"`
	InsuranceID     string    `json:"insurance_id,omitempty"`
	InsuranceCompany string   `json:"insurance_company,omitempty"`
	Active          bool      `json:"active"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}
