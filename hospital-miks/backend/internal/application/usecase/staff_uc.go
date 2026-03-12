// Package usecase implements application-level orchestration (driving side).
// Use cases coordinate domain entities and repository ports. Business rules
// live inside entities; use cases handle workflow and persistence calls.
package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// StaffUseCase orchestrates staff and department management.
type StaffUseCase struct {
	staffRepo port.StaffRepository
	deptRepo  port.DepartmentRepository
}

func NewStaffUseCase(s port.StaffRepository, d port.DepartmentRepository) *StaffUseCase {
	return &StaffUseCase{staffRepo: s, deptRepo: d}
}

func (uc *StaffUseCase) CreateStaff(ctx context.Context, staff *model.Staff) error {
	if err := staff.Validate(); err != nil {
		return err
	}
	staff.Touch()
	return uc.staffRepo.Create(ctx, staff)
}

func (uc *StaffUseCase) GetStaff(ctx context.Context, id uuid.UUID) (*model.Staff, error) {
	return uc.staffRepo.GetByID(ctx, id)
}

func (uc *StaffUseCase) UpdateStaff(ctx context.Context, staff *model.Staff) error {
	if err := staff.Validate(); err != nil {
		return err
	}
	staff.Touch()
	return uc.staffRepo.Update(ctx, staff)
}

func (uc *StaffUseCase) DeactivateStaff(ctx context.Context, id uuid.UUID) error {
	staff, err := uc.staffRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	staff.Deactivate()
	return uc.staffRepo.Update(ctx, staff)
}

func (uc *StaffUseCase) ListStaff(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Staff], error) {
	filter.Normalize()
	return uc.staffRepo.List(ctx, filter)
}

func (uc *StaffUseCase) ListStaffByDepartment(ctx context.Context, deptID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Staff], error) {
	filter.Normalize()
	return uc.staffRepo.ListByDepartment(ctx, deptID, filter)
}

// ── Departments ─────────────────────────────────────────────────

func (uc *StaffUseCase) CreateDepartment(ctx context.Context, dept *model.Department) error {
	if err := dept.Validate(); err != nil {
		return err
	}
	return uc.deptRepo.Create(ctx, dept)
}

func (uc *StaffUseCase) GetDepartment(ctx context.Context, id uuid.UUID) (*model.Department, error) {
	return uc.deptRepo.GetByID(ctx, id)
}

func (uc *StaffUseCase) ListDepartments(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Department], error) {
	filter.Normalize()
	return uc.deptRepo.List(ctx, filter)
}

func (uc *StaffUseCase) UpdateDepartment(ctx context.Context, dept *model.Department) error {
	if err := dept.Validate(); err != nil {
		return err
	}
	return uc.deptRepo.Update(ctx, dept)
}
