package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// RehabPlanRepository defines the outbound port for rehab plan persistence.
type RehabPlanRepository interface {
	Create(ctx context.Context, plan *model.RehabPlan) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.RehabPlan, error)
	Update(ctx context.Context, plan *model.RehabPlan) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.RehabPlan], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID) ([]model.RehabPlan, error)
	ListActive(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.RehabPlan], error)
}

// RehabSessionRepository defines the outbound port for rehab session persistence.
type RehabSessionRepository interface {
	Create(ctx context.Context, session *model.RehabSession) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.RehabSession, error)
	Update(ctx context.Context, session *model.RehabSession) error
	Cancel(ctx context.Context, id uuid.UUID) error
	ListByPlan(ctx context.Context, planID uuid.UUID) ([]model.RehabSession, error)
	ListByTherapist(ctx context.Context, therapistID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.RehabSession], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.RehabSession], error)
}
