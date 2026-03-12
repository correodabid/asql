package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// MessageRepository defines the outbound port for internal messaging.
type MessageRepository interface {
	Create(ctx context.Context, msg *model.Message) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Message, error)
	MarkRead(ctx context.Context, id uuid.UUID) error
	Delete(ctx context.Context, id uuid.UUID) error
	ListInbox(ctx context.Context, userID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Message], error)
	ListSent(ctx context.Context, userID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Message], error)
	CountUnread(ctx context.Context, userID uuid.UUID) (int, error)
	ListThread(ctx context.Context, parentID uuid.UUID) ([]model.Message, error)
}

// PatientCommunicationRepository defines the outbound port for patient comms.
type PatientCommunicationRepository interface {
	Create(ctx context.Context, comm *model.PatientCommunication) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.PatientCommunication, error)
	ListByPatient(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.PatientCommunication], error)
}
