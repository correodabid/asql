package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewAdmission creates a new Admission entity.
func NewAdmission(patientID, admittingDocID, bedID, departmentID uuid.UUID, diagnosis, reason string) (*Admission, error) {
	a := &Admission{
		ID:              uuid.New(),
		PatientID:       patientID,
		AdmittingDocID:  admittingDocID,
		BedID:           bedID,
		DepartmentID:    departmentID,
		Status:          AdmissionStatusAdmitted,
		AdmissionDate:   time.Now(),
		Diagnosis:       diagnosis,
		AdmissionReason: reason,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return a, nil
}

// Validate enforces Admission invariants.
func (a *Admission) Validate() error {
	if a.PatientID == uuid.Nil || a.AdmittingDocID == uuid.Nil {
		return fmt.Errorf("admission: patient and admitting doctor are required")
	}
	if a.BedID == uuid.Nil {
		return fmt.Errorf("admission: bed assignment is required")
	}
	return nil
}

// Discharge transitions admission to DISCHARGED.
func (a *Admission) Discharge() error {
	if a.Status == AdmissionStatusDischarged {
		return fmt.Errorf("admission: patient already discharged")
	}
	now := time.Now()
	a.Status = AdmissionStatusDischarged
	a.DischargeDate = &now
	a.UpdatedAt = now
	return nil
}

// IsActive returns true when patient is still admitted.
func (a *Admission) IsActive() bool {
	return a.Status == AdmissionStatusAdmitted || a.Status == AdmissionStatusInCare
}

// Occupy marks a bed as occupied.
func (b *Bed) Occupy() error {
	if b.Status != BedStatusAvailable {
		return fmt.Errorf("bed %s: not available (status: %s)", b.Number, b.Status)
	}
	b.Status = BedStatusOccupied
	b.UpdatedAt = time.Now()
	return nil
}

// Release puts a bed into cleaning status after patient leaves.
func (b *Bed) Release() {
	b.Status = BedStatusCleaning
	b.UpdatedAt = time.Now()
}

// MarkAvailable marks a bed as available.
func (b *Bed) MarkAvailable() {
	b.Status = BedStatusAvailable
	b.UpdatedAt = time.Now()
}

// NewMealOrder creates a new MealOrder entity.
func NewMealOrder(admissionID uuid.UUID, mealType MealType, date time.Time, menu, dietaryNote string) *MealOrder {
	return &MealOrder{
		ID:          uuid.New(),
		AdmissionID: admissionID,
		MealType:    mealType,
		Date:        date,
		Menu:        menu,
		DietaryNote: dietaryNote,
		Delivered:   false,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// MarkDelivered marks a meal as delivered.
func (m *MealOrder) MarkDelivered() {
	now := time.Now()
	m.Delivered = true
	m.DeliveredAt = &now
	m.UpdatedAt = now
}

// NewCareNote creates a new CareNote entity.
func NewCareNote(admissionID, staffID uuid.UUID, noteType, content string) *CareNote {
	return &CareNote{
		ID:          uuid.New(),
		AdmissionID: admissionID,
		StaffID:     staffID,
		NoteType:    noteType,
		Content:     content,
		CreatedAt:   time.Now(),
	}
}

// NewWard creates a new Ward entity.
func NewWard(name, code, building string, departmentID uuid.UUID, floor, totalBeds int) (*Ward, error) {
	w := &Ward{
		ID:           uuid.New(),
		Name:         name,
		Code:         code,
		DepartmentID: departmentID,
		Floor:        floor,
		Building:     building,
		TotalBeds:    totalBeds,
		Active:       true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if w.Name == "" || w.Code == "" {
		return nil, fmt.Errorf("ward: name and code are required")
	}
	return w, nil
}

// NewBed creates a new Bed entity.
func NewBed(wardID uuid.UUID, number, roomNo, features string) *Bed {
	return &Bed{
		ID:        uuid.New(),
		WardID:    wardID,
		Number:    number,
		Status:    BedStatusAvailable,
		RoomNo:    roomNo,
		Features:  features,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
