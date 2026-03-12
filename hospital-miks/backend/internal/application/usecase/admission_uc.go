package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// AdmissionUseCase orchestrates inpatient management: admissions, beds, meals, care notes.
type AdmissionUseCase struct {
	admRepo  port.AdmissionRepository
	wardRepo port.WardRepository
	bedRepo  port.BedRepository
	mealRepo port.MealOrderRepository
	careRepo port.CareNoteRepository
}

func NewAdmissionUseCase(a port.AdmissionRepository, w port.WardRepository, b port.BedRepository, m port.MealOrderRepository, c port.CareNoteRepository) *AdmissionUseCase {
	return &AdmissionUseCase{admRepo: a, wardRepo: w, bedRepo: b, mealRepo: m, careRepo: c}
}

// AdmitPatient validates the admission entity, checks bed availability, and persists.
func (uc *AdmissionUseCase) AdmitPatient(ctx context.Context, adm *model.Admission) error {
	if err := adm.Validate(); err != nil {
		return err
	}
	bed, err := uc.bedRepo.GetByID(ctx, adm.BedID)
	if err != nil {
		return err
	}
	if err := bed.Occupy(); err != nil {
		return err
	}
	if err := uc.admRepo.Create(ctx, adm); err != nil {
		return err
	}
	return uc.bedRepo.Update(ctx, bed)
}

// DischargePatient delegates discharge to the entity then releases the bed.
func (uc *AdmissionUseCase) DischargePatient(ctx context.Context, admissionID uuid.UUID) error {
	adm, err := uc.admRepo.GetByID(ctx, admissionID)
	if err != nil {
		return err
	}
	if err := adm.Discharge(); err != nil {
		return err
	}
	if err := uc.admRepo.Update(ctx, adm); err != nil {
		return err
	}
	bed, err := uc.bedRepo.GetByID(ctx, adm.BedID)
	if err != nil {
		return err
	}
	bed.Release()
	return uc.bedRepo.Update(ctx, bed)
}

func (uc *AdmissionUseCase) GetAdmission(ctx context.Context, id uuid.UUID) (*model.Admission, error) {
	return uc.admRepo.GetByID(ctx, id)
}

func (uc *AdmissionUseCase) ListActiveAdmissions(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Admission], error) {
	filter.Normalize()
	return uc.admRepo.ListActive(ctx, filter)
}

func (uc *AdmissionUseCase) ListPatientAdmissions(ctx context.Context, patientID uuid.UUID) ([]model.Admission, error) {
	return uc.admRepo.ListByPatient(ctx, patientID)
}

func (uc *AdmissionUseCase) OrderMeal(ctx context.Context, meal *model.MealOrder) error {
	return uc.mealRepo.Create(ctx, meal)
}

func (uc *AdmissionUseCase) GetMealOrders(ctx context.Context, admissionID uuid.UUID) ([]model.MealOrder, error) {
	return uc.mealRepo.ListByAdmission(ctx, admissionID)
}

func (uc *AdmissionUseCase) AddCareNote(ctx context.Context, note *model.CareNote) error {
	return uc.careRepo.Create(ctx, note)
}

func (uc *AdmissionUseCase) GetCareNotes(ctx context.Context, admissionID uuid.UUID) ([]model.CareNote, error) {
	return uc.careRepo.ListByAdmission(ctx, admissionID)
}

// ── Wards & Beds ────────────────────────────────────────────────

func (uc *AdmissionUseCase) CreateWard(ctx context.Context, ward *model.Ward) error {
	return uc.wardRepo.Create(ctx, ward)
}

func (uc *AdmissionUseCase) CreateBed(ctx context.Context, bed *model.Bed) error {
	return uc.bedRepo.Create(ctx, bed)
}

func (uc *AdmissionUseCase) ListWards(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Ward], error) {
	filter.Normalize()
	return uc.wardRepo.List(ctx, filter)
}

func (uc *AdmissionUseCase) ListBedsByWard(ctx context.Context, wardID uuid.UUID) ([]model.Bed, error) {
	return uc.bedRepo.ListByWard(ctx, wardID)
}

func (uc *AdmissionUseCase) ListAvailableBeds(ctx context.Context) ([]model.Bed, error) {
	return uc.bedRepo.ListAvailable(ctx)
}
