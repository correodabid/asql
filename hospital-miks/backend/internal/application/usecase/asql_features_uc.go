package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// ASQLFeaturesUseCase orchestrates ASQL-specific capabilities:
// time-travel queries, FOR HISTORY audit, and cross-domain reads via IMPORT.
type ASQLFeaturesUseCase struct {
	timeTravel  port.TimeTravelPort
	audit       port.AuditPort
	crossDomain port.CrossDomainReadPort
}

func NewASQLFeaturesUseCase(
	tt port.TimeTravelPort,
	a port.AuditPort,
	cd port.CrossDomainReadPort,
) *ASQLFeaturesUseCase {
	return &ASQLFeaturesUseCase{timeTravel: tt, audit: a, crossDomain: cd}
}

// ── Time Travel ─────────────────────────────────────────────────

func (uc *ASQLFeaturesUseCase) GetPatientSnapshot(ctx context.Context, id uuid.UUID, lsn uint64) (*port.PatientSnapshot, error) {
	return uc.timeTravel.GetPatientAsOfLSN(ctx, id, lsn)
}

func (uc *ASQLFeaturesUseCase) GetAdmissionSnapshot(ctx context.Context, id uuid.UUID, lsn uint64) (*port.AdmissionSnapshot, error) {
	return uc.timeTravel.GetAdmissionAsOfLSN(ctx, id, lsn)
}

func (uc *ASQLFeaturesUseCase) GetPrescriptionSnapshot(ctx context.Context, id uuid.UUID, lsn uint64) (*port.PrescriptionSnapshot, error) {
	return uc.timeTravel.GetPrescriptionAsOfLSN(ctx, id, lsn)
}

// ── Audit (FOR HISTORY) ─────────────────────────────────────────

func (uc *ASQLFeaturesUseCase) GetTableHistory(ctx context.Context, domain, table string) ([]port.ChangeRecord, error) {
	return uc.audit.GetTableHistory(ctx, domain, table)
}

func (uc *ASQLFeaturesUseCase) GetEntityHistory(ctx context.Context, domain, table string, id uuid.UUID) ([]port.ChangeRecord, error) {
	return uc.audit.GetEntityHistory(ctx, domain, table, id)
}

// ── Cross-Domain Reads (IMPORT) ─────────────────────────────────

func (uc *ASQLFeaturesUseCase) GetPatientWithInvoices(ctx context.Context, patientID uuid.UUID) (*port.PatientWithInvoices, error) {
	return uc.crossDomain.GetPatientWithInvoices(ctx, patientID)
}

func (uc *ASQLFeaturesUseCase) GetAppointmentWithDetails(ctx context.Context, appointmentID uuid.UUID) (*port.AppointmentWithDetails, error) {
	return uc.crossDomain.GetAppointmentWithDetails(ctx, appointmentID)
}
