package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// PatientUseCase orchestrates patient management workflows.
type PatientUseCase struct {
	repo port.PatientRepository
}

func NewPatientUseCase(r port.PatientRepository) *PatientUseCase {
	return &PatientUseCase{repo: r}
}

// RegisterPatient validates and registers a new patient, checking for duplicate NationalID.
func (uc *PatientUseCase) RegisterPatient(ctx context.Context, patient *model.Patient) error {
	if err := patient.Validate(); err != nil {
		return err
	}
	existing, err := uc.repo.GetByNationalID(ctx, patient.NationalID)
	if err == nil && existing != nil {
		return fmt.Errorf("patient with national ID %s already exists", patient.NationalID)
	}
	return uc.repo.Create(ctx, patient)
}

func (uc *PatientUseCase) GetPatient(ctx context.Context, id uuid.UUID) (*model.Patient, error) {
	return uc.repo.GetByID(ctx, id)
}

func (uc *PatientUseCase) GetPatientByMRN(ctx context.Context, mrn string) (*model.Patient, error) {
	return uc.repo.GetByMedicalRecordNo(ctx, mrn)
}

func (uc *PatientUseCase) UpdatePatient(ctx context.Context, patient *model.Patient) error {
	patient.Touch()
	return uc.repo.Update(ctx, patient)
}

func (uc *PatientUseCase) ListPatients(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Patient], error) {
	filter.Normalize()
	return uc.repo.List(ctx, filter)
}

func (uc *PatientUseCase) SearchPatients(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Patient], error) {
	filter.Normalize()
	return uc.repo.Search(ctx, query, filter)
}
