// Package model defines the core domain entities for Hospital MiKS.
package model

import (
	"time"

	"github.com/google/uuid"
)

// StaffType represents the type of hospital staff.
type StaffType string

const (
	StaffTypeDoctor     StaffType = "DOCTOR"
	StaffTypeNurse      StaffType = "NURSE"
	StaffTypeSurgeon    StaffType = "SURGEON"
	StaffTypeAdmin      StaffType = "ADMIN"
	StaffTypeTechnician StaffType = "TECHNICIAN"
	StaffTypePharmacist StaffType = "PHARMACIST"
	StaffTypeTherapist  StaffType = "THERAPIST"
	StaffTypeMaintenance StaffType = "MAINTENANCE"
	StaffTypeSecurity   StaffType = "SECURITY"
	StaffTypeOther      StaffType = "OTHER"
)

// Specialty represents a medical specialty.
type Specialty string

const (
	SpecialtyGeneralMedicine  Specialty = "GENERAL_MEDICINE"
	SpecialtyCardiology       Specialty = "CARDIOLOGY"
	SpecialtyNeurology        Specialty = "NEUROLOGY"
	SpecialtyOrthopedics      Specialty = "ORTHOPEDICS"
	SpecialtyPediatrics       Specialty = "PEDIATRICS"
	SpecialtyOncology         Specialty = "ONCOLOGY"
	SpecialtyDermatology      Specialty = "DERMATOLOGY"
	SpecialtyOphthalmology    Specialty = "OPHTHALMOLOGY"
	SpecialtyGynecology       Specialty = "GYNECOLOGY"
	SpecialtyUrology          Specialty = "UROLOGY"
	SpecialtyPsychiatry       Specialty = "PSYCHIATRY"
	SpecialtyRadiology        Specialty = "RADIOLOGY"
	SpecialtyAnesthesiology   Specialty = "ANESTHESIOLOGY"
	SpecialtyEmergency        Specialty = "EMERGENCY"
	SpecialtyRehabilitation   Specialty = "REHABILITATION"
	SpecialtyPhysiotherapy    Specialty = "PHYSIOTHERAPY"
	SpecialtyPharmacy         Specialty = "PHARMACY"
)

// Staff represents a hospital employee (medical or non-medical).
type Staff struct {
	ID            uuid.UUID  `json:"id"`
	EmployeeCode  string     `json:"employee_code"`
	FirstName     string     `json:"first_name"`
	LastName      string     `json:"last_name"`
	Email         string     `json:"email"`
	Phone         string     `json:"phone"`
	StaffType     StaffType  `json:"staff_type"`
	Specialty     Specialty  `json:"specialty,omitempty"`
	LicenseNumber string     `json:"license_number,omitempty"`
	DepartmentID  uuid.UUID  `json:"department_id"`
	HireDate      time.Time  `json:"hire_date"`
	Active        bool       `json:"active"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// Department represents a hospital department.
type Department struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Code      string    `json:"code"`
	Floor     int       `json:"floor"`
	Building  string    `json:"building"`
	HeadID    uuid.UUID `json:"head_id,omitempty"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
