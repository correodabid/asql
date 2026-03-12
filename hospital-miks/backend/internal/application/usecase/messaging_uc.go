package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// MessagingUseCase orchestrates internal messaging and patient communications.
type MessagingUseCase struct {
	msgRepo  port.MessageRepository
	commRepo port.PatientCommunicationRepository
}

func NewMessagingUseCase(mr port.MessageRepository, cr port.PatientCommunicationRepository) *MessagingUseCase {
	return &MessagingUseCase{msgRepo: mr, commRepo: cr}
}

func (uc *MessagingUseCase) SendMessage(ctx context.Context, msg *model.Message) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	return uc.msgRepo.Create(ctx, msg)
}

func (uc *MessagingUseCase) GetMessage(ctx context.Context, id uuid.UUID) (*model.Message, error) {
	return uc.msgRepo.GetByID(ctx, id)
}

// MarkAsRead delegates to entity method and persists.
func (uc *MessagingUseCase) MarkAsRead(ctx context.Context, id uuid.UUID) error {
	msg, err := uc.msgRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	msg.MarkRead()
	return uc.msgRepo.MarkRead(ctx, id)
}

func (uc *MessagingUseCase) GetInbox(ctx context.Context, userID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Message], error) {
	filter.Normalize()
	return uc.msgRepo.ListInbox(ctx, userID, filter)
}

func (uc *MessagingUseCase) GetSent(ctx context.Context, userID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Message], error) {
	filter.Normalize()
	return uc.msgRepo.ListSent(ctx, userID, filter)
}

func (uc *MessagingUseCase) GetUnreadCount(ctx context.Context, userID uuid.UUID) (int, error) {
	return uc.msgRepo.CountUnread(ctx, userID)
}

func (uc *MessagingUseCase) GetThread(ctx context.Context, parentID uuid.UUID) ([]model.Message, error) {
	return uc.msgRepo.ListThread(ctx, parentID)
}

// SendPatientCommunication creates an outbound patient communication (email, SMS, etc.).
func (uc *MessagingUseCase) SendPatientCommunication(ctx context.Context, comm *model.PatientCommunication) error {
	return uc.commRepo.Create(ctx, comm)
}

func (uc *MessagingUseCase) ListPatientCommunications(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.PatientCommunication], error) {
	filter.Normalize()
	return uc.commRepo.ListByPatient(ctx, patientID, filter)
}
