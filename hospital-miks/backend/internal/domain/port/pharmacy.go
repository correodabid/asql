package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// MedicationRepository defines the outbound port for medication persistence.
type MedicationRepository interface {
	Create(ctx context.Context, med *model.Medication) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Medication, error)
	GetByCode(ctx context.Context, code string) (*model.Medication, error)
	Update(ctx context.Context, med *model.Medication) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Medication], error)
	ListLowStock(ctx context.Context) ([]model.Medication, error)
	UpdateStock(ctx context.Context, id uuid.UUID, quantity int) error
	Search(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Medication], error)
}

// PrescriptionRepository defines the outbound port for prescription persistence.
type PrescriptionRepository interface {
	Create(ctx context.Context, rx *model.Prescription) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Prescription, error)
	Update(ctx context.Context, rx *model.Prescription) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Prescription], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID) ([]model.Prescription, error)
	ListActive(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Prescription], error)
}

// PharmacyDispenseRepository defines the outbound port for dispensation records.
type PharmacyDispenseRepository interface {
	Create(ctx context.Context, disp *model.PharmacyDispense) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.PharmacyDispense, error)
	ListByPrescription(ctx context.Context, rxID uuid.UUID) ([]model.PharmacyDispense, error)
}
