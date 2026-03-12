package port

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// OperatingRoomRepository defines the outbound port for operating room persistence.
type OperatingRoomRepository interface {
	Create(ctx context.Context, or *model.OperatingRoom) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.OperatingRoom, error)
	Update(ctx context.Context, or *model.OperatingRoom) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.OperatingRoom], error)
	ListAvailable(ctx context.Context, from, to time.Time) ([]model.OperatingRoom, error)
}

// SurgeryRepository defines the outbound port for surgery persistence.
type SurgeryRepository interface {
	Create(ctx context.Context, surgery *model.Surgery) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Surgery, error)
	Update(ctx context.Context, surgery *model.Surgery) error
	Cancel(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Surgery], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID) ([]model.Surgery, error)
	ListByDate(ctx context.Context, date time.Time) ([]model.Surgery, error)
	ListByOperatingRoom(ctx context.Context, orID uuid.UUID, date time.Time) ([]model.Surgery, error)
	AddTeamMember(ctx context.Context, member *model.SurgeryTeamMember) error
	GetTeamMembers(ctx context.Context, surgeryID uuid.UUID) ([]model.SurgeryTeamMember, error)
}
