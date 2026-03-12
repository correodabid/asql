package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// AdmissionRepository defines the outbound port for admission persistence.
type AdmissionRepository interface {
	Create(ctx context.Context, adm *model.Admission) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Admission, error)
	Update(ctx context.Context, adm *model.Admission) error
	Discharge(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Admission], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID) ([]model.Admission, error)
	ListActive(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Admission], error)
}

// WardRepository defines the outbound port for ward persistence.
type WardRepository interface {
	Create(ctx context.Context, ward *model.Ward) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Ward, error)
	Update(ctx context.Context, ward *model.Ward) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Ward], error)
}

// BedRepository defines the outbound port for bed persistence.
type BedRepository interface {
	Create(ctx context.Context, bed *model.Bed) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Bed, error)
	Update(ctx context.Context, bed *model.Bed) error
	Delete(ctx context.Context, id uuid.UUID) error
	ListByWard(ctx context.Context, wardID uuid.UUID) ([]model.Bed, error)
	ListAvailable(ctx context.Context) ([]model.Bed, error)
}

// MealOrderRepository defines the outbound port for meal order persistence.
type MealOrderRepository interface {
	Create(ctx context.Context, meal *model.MealOrder) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.MealOrder, error)
	Update(ctx context.Context, meal *model.MealOrder) error
	ListByAdmission(ctx context.Context, admID uuid.UUID) ([]model.MealOrder, error)
}

// CareNoteRepository defines the outbound port for care note persistence.
type CareNoteRepository interface {
	Create(ctx context.Context, note *model.CareNote) error
	ListByAdmission(ctx context.Context, admID uuid.UUID) ([]model.CareNote, error)
}
