package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GenerateMRN generates a medical record number.
func GenerateMRN() string {
	return fmt.Sprintf("MRN-%s", uuid.New().String()[:8])
}

// NewPatient creates a new Patient entity with validation.
func NewPatient(firstName, lastName, nationalID, phone, address, city, postalCode string, dob time.Time, gender Gender) (*Patient, error) {
	p := &Patient{
		ID:              uuid.New(),
		MedicalRecordNo: GenerateMRN(),
		FirstName:       firstName,
		LastName:        lastName,
		DateOfBirth:     dob,
		Gender:          gender,
		NationalID:      nationalID,
		Phone:           phone,
		Address:         address,
		City:            city,
		PostalCode:      postalCode,
		Active:          true,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// Validate enforces Patient domain invariants.
func (p *Patient) Validate() error {
	if p.FirstName == "" || p.LastName == "" {
		return fmt.Errorf("patient: first name and last name are required")
	}
	if p.NationalID == "" {
		return fmt.Errorf("patient: national ID is required")
	}
	return nil
}

// Touch updates the UpdatedAt timestamp.
func (p *Patient) Touch() {
	p.UpdatedAt = time.Now()
}
