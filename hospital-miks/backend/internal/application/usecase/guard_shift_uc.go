package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// GuardShiftUseCase orchestrates guard shift management.
type GuardShiftUseCase struct {
	repo port.GuardShiftRepository
}

func NewGuardShiftUseCase(r port.GuardShiftRepository) *GuardShiftUseCase {
	return &GuardShiftUseCase{repo: r}
}

func (uc *GuardShiftUseCase) CreateShift(ctx context.Context, shift *model.GuardShift) error {
	if err := shift.Validate(); err != nil {
		return err
	}
	return uc.repo.Create(ctx, shift)
}

func (uc *GuardShiftUseCase) GetShift(ctx context.Context, id uuid.UUID) (*model.GuardShift, error) {
	return uc.repo.GetByID(ctx, id)
}

// SwapShift uses the entity's Swap method to enforce domain rules.
func (uc *GuardShiftUseCase) SwapShift(ctx context.Context, shiftID, newStaffID uuid.UUID) error {
	shift, err := uc.repo.GetByID(ctx, shiftID)
	if err != nil {
		return err
	}
	if err := shift.Swap(newStaffID); err != nil {
		return err
	}
	return uc.repo.Update(ctx, shift)
}

func (uc *GuardShiftUseCase) ListShifts(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.GuardShift], error) {
	filter.Normalize()
	return uc.repo.List(ctx, filter)
}

func (uc *GuardShiftUseCase) ListStaffShifts(ctx context.Context, staffID uuid.UUID, from, to time.Time) ([]model.GuardShift, error) {
	return uc.repo.ListByStaff(ctx, staffID, from, to)
}

func (uc *GuardShiftUseCase) ListDepartmentShifts(ctx context.Context, deptID uuid.UUID, from, to time.Time) ([]model.GuardShift, error) {
	return uc.repo.ListByDepartment(ctx, deptID, from, to)
}
