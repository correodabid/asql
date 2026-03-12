package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// PharmacyUseCase orchestrates medication, prescription and dispensation workflows.
type PharmacyUseCase struct {
	medRepo  port.MedicationRepository
	rxRepo   port.PrescriptionRepository
	dispRepo port.PharmacyDispenseRepository
}

func NewPharmacyUseCase(m port.MedicationRepository, r port.PrescriptionRepository, d port.PharmacyDispenseRepository) *PharmacyUseCase {
	return &PharmacyUseCase{medRepo: m, rxRepo: r, dispRepo: d}
}

func (uc *PharmacyUseCase) AddMedication(ctx context.Context, med *model.Medication) error {
	if err := med.Validate(); err != nil {
		return err
	}
	return uc.medRepo.Create(ctx, med)
}

func (uc *PharmacyUseCase) GetMedication(ctx context.Context, id uuid.UUID) (*model.Medication, error) {
	return uc.medRepo.GetByID(ctx, id)
}

func (uc *PharmacyUseCase) UpdateMedication(ctx context.Context, med *model.Medication) error {
	if err := med.Validate(); err != nil {
		return err
	}
	return uc.medRepo.Update(ctx, med)
}

func (uc *PharmacyUseCase) ListMedications(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Medication], error) {
	filter.Normalize()
	return uc.medRepo.List(ctx, filter)
}

func (uc *PharmacyUseCase) SearchMedications(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Medication], error) {
	filter.Normalize()
	return uc.medRepo.Search(ctx, query, filter)
}

func (uc *PharmacyUseCase) GetLowStockMedications(ctx context.Context) ([]model.Medication, error) {
	return uc.medRepo.ListLowStock(ctx)
}

// ── Prescriptions ───────────────────────────────────────────────

func (uc *PharmacyUseCase) CreatePrescription(ctx context.Context, rx *model.Prescription) error {
	if err := rx.Validate(); err != nil {
		return err
	}
	return uc.rxRepo.Create(ctx, rx)
}

func (uc *PharmacyUseCase) GetPrescription(ctx context.Context, id uuid.UUID) (*model.Prescription, error) {
	return uc.rxRepo.GetByID(ctx, id)
}

func (uc *PharmacyUseCase) ListPatientPrescriptions(ctx context.Context, patientID uuid.UUID) ([]model.Prescription, error) {
	return uc.rxRepo.ListByPatient(ctx, patientID)
}

func (uc *PharmacyUseCase) ListActivePrescriptions(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Prescription], error) {
	filter.Normalize()
	return uc.rxRepo.ListActive(ctx, filter)
}

func (uc *PharmacyUseCase) ListDispensesByPrescription(ctx context.Context, rxID uuid.UUID) ([]model.PharmacyDispense, error) {
	return uc.dispRepo.ListByPrescription(ctx, rxID)
}

// DispensePrescription orchestrates: validate stock → create dispense → deduct stock → mark dispensed.
func (uc *PharmacyUseCase) DispensePrescription(ctx context.Context, rxID, pharmacistID uuid.UUID, quantity int, notes string) error {
	rx, err := uc.rxRepo.GetByID(ctx, rxID)
	if err != nil {
		return fmt.Errorf("prescription not found: %w", err)
	}
	med, err := uc.medRepo.GetByID(ctx, rx.MedicationID)
	if err != nil {
		return fmt.Errorf("medication not found: %w", err)
	}

	// Entity-level validations
	if err := med.DeductStock(quantity); err != nil {
		return err
	}
	if err := rx.MarkDispensed(); err != nil {
		return err
	}

	// Persist
	disp := model.NewDispense(rxID, pharmacistID, quantity, notes)
	if err := uc.dispRepo.Create(ctx, disp); err != nil {
		return fmt.Errorf("creating dispense record: %w", err)
	}
	if err := uc.medRepo.UpdateStock(ctx, med.ID, med.StockQuantity); err != nil {
		return fmt.Errorf("updating stock: %w", err)
	}
	return uc.rxRepo.Update(ctx, rx)
}
