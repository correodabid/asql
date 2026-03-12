package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// SurgeryUseCase orchestrates OR and surgery workflows.
type SurgeryUseCase struct {
	orRepo      port.OperatingRoomRepository
	surgeryRepo port.SurgeryRepository
}

func NewSurgeryUseCase(or port.OperatingRoomRepository, s port.SurgeryRepository) *SurgeryUseCase {
	return &SurgeryUseCase{orRepo: or, surgeryRepo: s}
}

func (uc *SurgeryUseCase) CreateOperatingRoom(ctx context.Context, or *model.OperatingRoom) error {
	if err := or.Validate(); err != nil {
		return err
	}
	return uc.orRepo.Create(ctx, or)
}

func (uc *SurgeryUseCase) ListOperatingRooms(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.OperatingRoom], error) {
	filter.Normalize()
	return uc.orRepo.List(ctx, filter)
}

// ScheduleSurgery validates, checks OR availability, and persists.
func (uc *SurgeryUseCase) ScheduleSurgery(ctx context.Context, surgery *model.Surgery) error {
	if err := surgery.Validate(); err != nil {
		return err
	}
	existing, err := uc.surgeryRepo.ListByOperatingRoom(ctx, surgery.OperatingRoomID, surgery.ScheduledStart)
	if err != nil {
		return fmt.Errorf("checking OR availability: %w", err)
	}
	for _, e := range existing {
		if surgery.ConflictsWith(e) {
			return fmt.Errorf("operating room conflict with surgery %s", e.ID)
		}
	}
	return uc.surgeryRepo.Create(ctx, surgery)
}

func (uc *SurgeryUseCase) GetSurgery(ctx context.Context, id uuid.UUID) (*model.Surgery, error) {
	return uc.surgeryRepo.GetByID(ctx, id)
}

// StartSurgery delegates the state transition to the entity.
func (uc *SurgeryUseCase) StartSurgery(ctx context.Context, id uuid.UUID) error {
	surgery, err := uc.surgeryRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := surgery.Start(); err != nil {
		return err
	}
	return uc.surgeryRepo.Update(ctx, surgery)
}

// CompleteSurgery delegates to entity.
func (uc *SurgeryUseCase) CompleteSurgery(ctx context.Context, id uuid.UUID, postOpNotes, complications string) error {
	surgery, err := uc.surgeryRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := surgery.Complete(postOpNotes, complications); err != nil {
		return err
	}
	return uc.surgeryRepo.Update(ctx, surgery)
}

func (uc *SurgeryUseCase) ListSurgeries(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Surgery], error) {
	filter.Normalize()
	return uc.surgeryRepo.List(ctx, filter)
}

func (uc *SurgeryUseCase) ListPatientSurgeries(ctx context.Context, patientID uuid.UUID) ([]model.Surgery, error) {
	return uc.surgeryRepo.ListByPatient(ctx, patientID)
}

func (uc *SurgeryUseCase) AddTeamMember(ctx context.Context, surgeryID, staffID uuid.UUID, role string) error {
	member := &model.SurgeryTeamMember{
		ID:        uuid.New(),
		SurgeryID: surgeryID,
		StaffID:   staffID,
		Role:      role,
	}
	return uc.surgeryRepo.AddTeamMember(ctx, member)
}

func (uc *SurgeryUseCase) GetTeamMembers(ctx context.Context, surgeryID uuid.UUID) ([]model.SurgeryTeamMember, error) {
	return uc.surgeryRepo.GetTeamMembers(ctx, surgeryID)
}
