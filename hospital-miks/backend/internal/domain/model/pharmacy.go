package model

import (
	"time"

	"github.com/google/uuid"
)

// MedicationCategory represents a category of medication.
type MedicationCategory string

const (
	MedCatAnalgesic     MedicationCategory = "ANALGESIC"
	MedCatAntibiotic    MedicationCategory = "ANTIBIOTIC"
	MedCatAntiInflam    MedicationCategory = "ANTI_INFLAMMATORY"
	MedCatCardiovascular MedicationCategory = "CARDIOVASCULAR"
	MedCatHormonal      MedicationCategory = "HORMONAL"
	MedCatPsychiatric   MedicationCategory = "PSYCHIATRIC"
	MedCatOncologic     MedicationCategory = "ONCOLOGIC"
	MedCatVaccine       MedicationCategory = "VACCINE"
	MedCatOther         MedicationCategory = "OTHER"
)

// PrescriptionStatus represents the status of a prescription.
type PrescriptionStatus string

const (
	PrescriptionStatusActive    PrescriptionStatus = "ACTIVE"
	PrescriptionStatusDispensed PrescriptionStatus = "DISPENSED"
	PrescriptionStatusCancelled PrescriptionStatus = "CANCELLED"
	PrescriptionStatusExpired   PrescriptionStatus = "EXPIRED"
)

// Medication represents a pharmaceutical product.
type Medication struct {
	ID             uuid.UUID          `json:"id"`
	Name           string             `json:"name"`
	GenericName    string             `json:"generic_name"`
	Code           string             `json:"code"`
	Category       MedicationCategory `json:"category"`
	Manufacturer   string             `json:"manufacturer"`
	DosageForm     string             `json:"dosage_form"` // tablet, capsule, injection, etc.
	Strength       string             `json:"strength"`    // e.g. "500mg"
	Unit           string             `json:"unit"`
	StockQuantity  int                `json:"stock_quantity"`
	MinStock       int                `json:"min_stock"`
	Price          float64            `json:"price"`
	RequiresRx     bool               `json:"requires_prescription"`
	Controlled     bool               `json:"controlled"`
	ExpirationDate time.Time          `json:"expiration_date"`
	Active         bool               `json:"active"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// Prescription represents a medical prescription.
type Prescription struct {
	ID           uuid.UUID          `json:"id"`
	PatientID    uuid.UUID          `json:"patient_id"`
	DoctorID     uuid.UUID          `json:"doctor_id"`
	MedicationID uuid.UUID          `json:"medication_id"`
	Status       PrescriptionStatus `json:"status"`
	Dosage       string             `json:"dosage"`
	Frequency    string             `json:"frequency"`
	Duration     string             `json:"duration"`
	Instructions string             `json:"instructions,omitempty"`
	Quantity     int                `json:"quantity"`
	Refills      int                `json:"refills_allowed"`
	RefillsUsed  int                `json:"refills_used"`
	PrescribedAt time.Time          `json:"prescribed_at"`
	DispensedAt  *time.Time         `json:"dispensed_at,omitempty"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

// PharmacyDispense represents a medication dispensation record.
type PharmacyDispense struct {
	ID             uuid.UUID `json:"id"`
	PrescriptionID uuid.UUID `json:"prescription_id"`
	PharmacistID   uuid.UUID `json:"pharmacist_id"`
	Quantity       int       `json:"quantity"`
	Notes          string    `json:"notes,omitempty"`
	DispensedAt    time.Time `json:"dispensed_at"`
	CreatedAt      time.Time `json:"created_at"`
}
