package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// PatientRepository defines the outbound port for patient persistence.
type PatientRepository interface {
	Create(ctx context.Context, patient *model.Patient) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Patient, error)
	GetByMedicalRecordNo(ctx context.Context, mrn string) (*model.Patient, error)
	GetByNationalID(ctx context.Context, nid string) (*model.Patient, error)
	Update(ctx context.Context, patient *model.Patient) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Patient], error)
	Search(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Patient], error)
}
