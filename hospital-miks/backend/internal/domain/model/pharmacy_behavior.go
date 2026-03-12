package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewMedication creates a new Medication entity.
func NewMedication(name, genericName, code string, category MedicationCategory, manufacturer, dosageForm, strength, unit string, stockQty, minStock int, price float64, requiresRx, controlled bool, expDate time.Time) (*Medication, error) {
	m := &Medication{
		ID:             uuid.New(),
		Name:           name,
		GenericName:    genericName,
		Code:           code,
		Category:       category,
		Manufacturer:   manufacturer,
		DosageForm:     dosageForm,
		Strength:       strength,
		Unit:           unit,
		StockQuantity:  stockQty,
		MinStock:       minStock,
		Price:          price,
		RequiresRx:     requiresRx,
		Controlled:     controlled,
		ExpirationDate: expDate,
		Active:         true,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// Validate enforces Medication invariants.
func (m *Medication) Validate() error {
	if m.Name == "" || m.Code == "" {
		return fmt.Errorf("medication: name and code are required")
	}
	if m.Price < 0 {
		return fmt.Errorf("medication: price cannot be negative")
	}
	return nil
}

// IsLowStock returns true if stock is at or below the minimum threshold.
func (m *Medication) IsLowStock() bool {
	return m.StockQuantity <= m.MinStock
}

// DeductStock reduces stock by qty, enforcing non-negative invariant.
func (m *Medication) DeductStock(qty int) error {
	if qty <= 0 {
		return fmt.Errorf("medication: deduction quantity must be positive")
	}
	if m.StockQuantity < qty {
		return fmt.Errorf("medication: insufficient stock (available %d, requested %d)", m.StockQuantity, qty)
	}
	m.StockQuantity -= qty
	m.UpdatedAt = time.Now()
	return nil
}

// SetStock sets stock to an exact quantity.
func (m *Medication) SetStock(qty int) {
	m.StockQuantity = qty
	m.UpdatedAt = time.Now()
}

// NewPrescription creates a new Prescription entity.
func NewPrescription(patientID, doctorID, medicationID uuid.UUID, dosage, frequency, duration, instructions string, quantity, refills int) (*Prescription, error) {
	rx := &Prescription{
		ID:           uuid.New(),
		PatientID:    patientID,
		DoctorID:     doctorID,
		MedicationID: medicationID,
		Status:       PrescriptionStatusActive,
		Dosage:       dosage,
		Frequency:    frequency,
		Duration:     duration,
		Instructions: instructions,
		Quantity:     quantity,
		Refills:      refills,
		RefillsUsed:  0,
		PrescribedAt: time.Now(),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if err := rx.Validate(); err != nil {
		return nil, err
	}
	return rx, nil
}

// Validate enforces Prescription invariants.
func (rx *Prescription) Validate() error {
	if rx.PatientID == uuid.Nil || rx.DoctorID == uuid.Nil || rx.MedicationID == uuid.Nil {
		return fmt.Errorf("prescription: patient, doctor, and medication are required")
	}
	if rx.Dosage == "" || rx.Frequency == "" {
		return fmt.Errorf("prescription: dosage and frequency are required")
	}
	return nil
}

// MarkDispensed transitions the prescription to DISPENSED.
func (rx *Prescription) MarkDispensed() error {
	if rx.Status != PrescriptionStatusActive {
		return fmt.Errorf("prescription: cannot dispense from status %s", rx.Status)
	}
	now := time.Now()
	rx.Status = PrescriptionStatusDispensed
	rx.DispensedAt = &now
	rx.RefillsUsed++
	rx.UpdatedAt = now
	return nil
}

// Cancel cancels the prescription.
func (rx *Prescription) Cancel() error {
	if rx.Status != PrescriptionStatusActive {
		return fmt.Errorf("prescription: cannot cancel from status %s", rx.Status)
	}
	rx.Status = PrescriptionStatusCancelled
	rx.UpdatedAt = time.Now()
	return nil
}

// NewDispense creates a new PharmacyDispense record.
func NewDispense(prescriptionID, pharmacistID uuid.UUID, quantity int, notes string) *PharmacyDispense {
	return &PharmacyDispense{
		ID:             uuid.New(),
		PrescriptionID: prescriptionID,
		PharmacistID:   pharmacistID,
		Quantity:       quantity,
		Notes:          notes,
		DispensedAt:    time.Now(),
		CreatedAt:      time.Now(),
	}
}
