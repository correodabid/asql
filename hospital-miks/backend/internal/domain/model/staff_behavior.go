package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewStaff creates a new Staff entity with generated ID and timestamps.
func NewStaff(employeeCode, firstName, lastName, email, phone string, staffType StaffType, specialty Specialty, licenseNumber string, departmentID uuid.UUID, hireDate time.Time) (*Staff, error) {
	s := &Staff{
		ID:            uuid.New(),
		EmployeeCode:  employeeCode,
		FirstName:     firstName,
		LastName:      lastName,
		Email:         email,
		Phone:         phone,
		StaffType:     staffType,
		Specialty:     specialty,
		LicenseNumber: licenseNumber,
		DepartmentID:  departmentID,
		HireDate:      hireDate,
		Active:        true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Validate enforces Staff invariants.
func (s *Staff) Validate() error {
	if s.FirstName == "" || s.LastName == "" {
		return fmt.Errorf("staff: first name and last name are required")
	}
	if s.Email == "" {
		return fmt.Errorf("staff: email is required")
	}
	if s.DepartmentID == uuid.Nil {
		return fmt.Errorf("staff: department is required")
	}
	return nil
}

// Deactivate soft-deletes a staff member.
func (s *Staff) Deactivate() {
	s.Active = false
	s.UpdatedAt = time.Now()
}

// Touch updates the UpdatedAt timestamp.
func (s *Staff) Touch() {
	s.UpdatedAt = time.Now()
}

// NewDepartment creates a new Department entity.
func NewDepartment(name, code, building string, floor int) (*Department, error) {
	d := &Department{
		ID:        uuid.New(),
		Name:      name,
		Code:      code,
		Floor:     floor,
		Building:  building,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// Validate enforces Department invariants.
func (d *Department) Validate() error {
	if d.Name == "" || d.Code == "" {
		return fmt.Errorf("department: name and code are required")
	}
	return nil
}
