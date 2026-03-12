package port

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// GuardShiftRepository defines the outbound port for guard shift persistence.
type GuardShiftRepository interface {
	Create(ctx context.Context, shift *model.GuardShift) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.GuardShift, error)
	Update(ctx context.Context, shift *model.GuardShift) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.GuardShift], error)
	ListByStaff(ctx context.Context, staffID uuid.UUID, from, to time.Time) ([]model.GuardShift, error)
	ListByDepartment(ctx context.Context, deptID uuid.UUID, from, to time.Time) ([]model.GuardShift, error)
	ListByDate(ctx context.Context, date time.Time) ([]model.GuardShift, error)
}
