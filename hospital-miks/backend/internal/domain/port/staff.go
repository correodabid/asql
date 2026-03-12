// Package port defines the driven (outbound) and driving (inbound) interfaces
// for the Hospital MiKS application following hexagonal architecture.
package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// StaffRepository defines the outbound port for staff persistence.
type StaffRepository interface {
	Create(ctx context.Context, staff *model.Staff) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Staff, error)
	GetByEmployeeCode(ctx context.Context, code string) (*model.Staff, error)
	Update(ctx context.Context, staff *model.Staff) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Staff], error)
	ListByDepartment(ctx context.Context, deptID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Staff], error)
	ListByType(ctx context.Context, staffType model.StaffType, filter model.ListFilter) (*model.ListResult[model.Staff], error)
}

// DepartmentRepository defines the outbound port for department persistence.
type DepartmentRepository interface {
	Create(ctx context.Context, dept *model.Department) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Department, error)
	Update(ctx context.Context, dept *model.Department) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Department], error)
}
