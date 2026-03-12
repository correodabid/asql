package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// RehabUseCase orchestrates rehabilitation plan and session management.
type RehabUseCase struct {
	planRepo    port.RehabPlanRepository
	sessionRepo port.RehabSessionRepository
}

func NewRehabUseCase(pr port.RehabPlanRepository, sr port.RehabSessionRepository) *RehabUseCase {
	return &RehabUseCase{planRepo: pr, sessionRepo: sr}
}

func (uc *RehabUseCase) CreatePlan(ctx context.Context, plan *model.RehabPlan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	return uc.planRepo.Create(ctx, plan)
}

func (uc *RehabUseCase) GetPlan(ctx context.Context, id uuid.UUID) (*model.RehabPlan, error) {
	return uc.planRepo.GetByID(ctx, id)
}

func (uc *RehabUseCase) UpdatePlan(ctx context.Context, plan *model.RehabPlan) error {
	return uc.planRepo.Update(ctx, plan)
}

func (uc *RehabUseCase) ListPatientPlans(ctx context.Context, patientID uuid.UUID) ([]model.RehabPlan, error) {
	return uc.planRepo.ListByPatient(ctx, patientID)
}

func (uc *RehabUseCase) ListActivePlans(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.RehabPlan], error) {
	filter.Normalize()
	return uc.planRepo.ListActive(ctx, filter)
}

// ScheduleSession creates a new rehabilitation session.
func (uc *RehabUseCase) ScheduleSession(ctx context.Context, session *model.RehabSession) error {
	return uc.sessionRepo.Create(ctx, session)
}

// CompleteSession marks a session as completed and increments the plan's completed count.
func (uc *RehabUseCase) CompleteSession(ctx context.Context, sessionID uuid.UUID, progress, exercises, notes string, painLevel *int) error {
	session, err := uc.sessionRepo.GetByID(ctx, sessionID)
	if err != nil {
		return err
	}
	if err := session.Complete(progress, exercises, notes, painLevel); err != nil {
		return err
	}
	if err := uc.sessionRepo.Update(ctx, session); err != nil {
		return err
	}

	// Increment plan's completed count.
	plan, err := uc.planRepo.GetByID(ctx, session.PlanID)
	if err != nil {
		return err
	}
	plan.IncrementCompleted()
	return uc.planRepo.Update(ctx, plan)
}

// CancelSession delegates to entity and persists.
func (uc *RehabUseCase) CancelSession(ctx context.Context, sessionID uuid.UUID) error {
	session, err := uc.sessionRepo.GetByID(ctx, sessionID)
	if err != nil {
		return err
	}
	if err := session.Cancel(); err != nil {
		return err
	}
	return uc.sessionRepo.Update(ctx, session)
}

func (uc *RehabUseCase) ListPlanSessions(ctx context.Context, planID uuid.UUID) ([]model.RehabSession, error) {
	return uc.sessionRepo.ListByPlan(ctx, planID)
}
