package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ── Time Travel Port ────────────────────────────────────────────────────────

// TimeTravelPort provides historical queries using ASQL's AS OF LSN feature.
// Every entity repository can optionally support time-travel reads.
type TimeTravelPort interface {
	// GetPatientAsOfLSN returns the patient record at a historical LSN.
	GetPatientAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*PatientSnapshot, error)

	// GetAdmissionAsOfLSN returns the admission record at a historical LSN.
	GetAdmissionAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*AdmissionSnapshot, error)

	// GetPrescriptionAsOfLSN returns the prescription record at a historical LSN.
	GetPrescriptionAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*PrescriptionSnapshot, error)
}

// PatientSnapshot is a point-in-time view of a patient.
type PatientSnapshot struct {
	LSN       uint64
	Timestamp time.Time
	Patient   map[string]string // column→value
}

// AdmissionSnapshot is a point-in-time view of an admission.
type AdmissionSnapshot struct {
	LSN       uint64
	Timestamp time.Time
	Data      map[string]string
}

// PrescriptionSnapshot is a point-in-time view of a prescription.
type PrescriptionSnapshot struct {
	LSN       uint64
	Timestamp time.Time
	Data      map[string]string
}

// ── Audit Port ──────────────────────────────────────────────────────────────

// AuditPort exposes ASQL's FOR HISTORY feature for compliance auditing.
type AuditPort interface {
	// GetTableHistory returns the complete change log of a table in a domain.
	GetTableHistory(ctx context.Context, domain, table string) ([]ChangeRecord, error)

	// GetEntityHistory returns the change log for a specific entity by ID.
	GetEntityHistory(ctx context.Context, domain, table string, id uuid.UUID) ([]ChangeRecord, error)
}

// ChangeRecord represents a single row from FOR HISTORY.
type ChangeRecord struct {
	Operation string            // INSERT, UPDATE, DELETE
	CommitLSN uint64            // WAL position
	Columns   map[string]string // column name → value (as string)
}

// ── Cross-Domain Read Port ──────────────────────────────────────────────────

// CrossDomainReadPort enables cross-domain reads using ASQL's IMPORT feature.
type CrossDomainReadPort interface {
	// GetPatientWithInvoices reads patient info from the patients domain
	// and their invoices from the billing domain using IMPORT.
	GetPatientWithInvoices(ctx context.Context, patientID uuid.UUID) (*PatientWithInvoices, error)

	// GetAppointmentWithDetails reads appointment info enriched with
	// patient name and doctor name from their respective domains.
	GetAppointmentWithDetails(ctx context.Context, appointmentID uuid.UUID) (*AppointmentWithDetails, error)
}

// PatientWithInvoices is a cross-domain view.
type PatientWithInvoices struct {
	PatientID     uuid.UUID
	FirstName     string
	LastName      string
	InvoiceCount  int
	TotalBilled   float64
}

// AppointmentWithDetails is a cross-domain enriched view.
type AppointmentWithDetails struct {
	AppointmentID uuid.UUID
	PatientName   string
	DoctorName    string
	Type          string
	Status        string
	ScheduledAt   time.Time
}
