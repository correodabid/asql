package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// ── TimeTravelRepo — implements port.TimeTravelPort ─────────────────────────

type TimeTravelRepo struct{ client *Client }

func NewTimeTravelRepo(c *Client) *TimeTravelRepo { return &TimeTravelRepo{client: c} }

func (r *TimeTravelRepo) GetPatientAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*port.PatientSnapshot, error) {
	sql := fmt.Sprintf("SELECT * FROM patients WHERE id = %s", sqlStr(id.String()))
	rows, err := r.client.TimeTravelQuery(ctx, sql, lsn)
	if err != nil {
		return nil, fmt.Errorf("time-travel patient query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("patient not found at LSN %d: %s", lsn, id)
	}
	data, err := scanToMap(rows)
	if err != nil {
		return nil, err
	}
	snap := &port.PatientSnapshot{
		LSN:     lsn,
		Patient: data,
	}
	return snap, nil
}

func (r *TimeTravelRepo) GetAdmissionAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*port.AdmissionSnapshot, error) {
	sql := fmt.Sprintf("SELECT * FROM admissions WHERE id = %s", sqlStr(id.String()))
	rows, err := r.client.TimeTravelQuery(ctx, sql, lsn)
	if err != nil {
		return nil, fmt.Errorf("time-travel admission query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("admission not found at LSN %d: %s", lsn, id)
	}
	data, err := scanToMap(rows)
	if err != nil {
		return nil, err
	}
	snap := &port.AdmissionSnapshot{
		LSN:  lsn,
		Data: data,
	}
	return snap, nil
}

func (r *TimeTravelRepo) GetPrescriptionAsOfLSN(ctx context.Context, id uuid.UUID, lsn uint64) (*port.PrescriptionSnapshot, error) {
	sql := fmt.Sprintf("SELECT * FROM prescriptions WHERE id = %s", sqlStr(id.String()))
	rows, err := r.client.TimeTravelQuery(ctx, sql, lsn)
	if err != nil {
		return nil, fmt.Errorf("time-travel prescription query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("prescription not found at LSN %d: %s", lsn, id)
	}
	data, err := scanToMap(rows)
	if err != nil {
		return nil, err
	}
	snap := &port.PrescriptionSnapshot{
		LSN:  lsn,
		Data: data,
	}
	return snap, nil
}

// ── AuditRepo — implements port.AuditPort ───────────────────────────────────

type AuditRepo struct{ client *Client }

func NewAuditRepo(c *Client) *AuditRepo { return &AuditRepo{client: c} }

func (r *AuditRepo) GetTableHistory(ctx context.Context, domain, table string) ([]port.ChangeRecord, error) {
	history, err := r.client.ForHistory(ctx, domain, table)
	if err != nil {
		return nil, fmt.Errorf("audit table history: %w", err)
	}
	records := make([]port.ChangeRecord, len(history))
	for i, h := range history {
		records[i] = port.ChangeRecord{
			Operation: h.Operation,
			CommitLSN: h.CommitLSN,
			Columns:   h.Columns,
		}
	}
	return records, nil
}

func (r *AuditRepo) GetEntityHistory(ctx context.Context, domain, table string, id uuid.UUID) ([]port.ChangeRecord, error) {
	// First get all history, then filter by ID
	history, err := r.client.ForHistory(ctx, domain, table)
	if err != nil {
		return nil, fmt.Errorf("audit entity history: %w", err)
	}
	idStr := id.String()
	var records []port.ChangeRecord
	for _, h := range history {
		// Check if this row's id column matches
		if h.Columns["id"] == idStr {
			records = append(records, port.ChangeRecord{
				Operation: h.Operation,
				CommitLSN: h.CommitLSN,
				Columns:   h.Columns,
			})
		}
	}
	return records, nil
}

// ── CrossDomainReadRepo — implements port.CrossDomainReadPort ────────────────

type CrossDomainReadRepo struct{ client *Client }

func NewCrossDomainReadRepo(c *Client) *CrossDomainReadRepo {
	return &CrossDomainReadRepo{client: c}
}

func (r *CrossDomainReadRepo) GetPatientWithInvoices(ctx context.Context, patientID uuid.UUID) (*port.PatientWithInvoices, error) {
	// Use IMPORT to query billing domain from within a patients domain tx
	idStr := sqlStr(patientID.String())

	// First get patient info from patients domain
	tx, err := r.client.BeginDomain(ctx, "patients")
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	pRows, err := tx.Query(ctx, fmt.Sprintf("SELECT first_name, last_name FROM patients WHERE id = %s", idStr))
	if err != nil {
		return nil, err
	}
	defer pRows.Close()
	if !pRows.Next() {
		return nil, fmt.Errorf("patient not found: %s", patientID)
	}
	pVals, err := pRows.Values()
	if err != nil {
		return nil, err
	}
	firstName := fmt.Sprintf("%v", pVals[0])
	lastName := fmt.Sprintf("%v", pVals[1])
	pRows.Close()

	// Now use IMPORT to read invoices from billing domain
	importSQL := fmt.Sprintf(
		"IMPORT billing.invoices; SELECT COUNT(*) AS cnt, COALESCE(SUM(total), 0) AS total_billed FROM invoices WHERE patient_id = %s",
		idStr,
	)
	iRows, err := r.client.ImportQuery(ctx, "patients", importSQL)
	if err != nil {
		return nil, fmt.Errorf("cross-domain invoice query: %w", err)
	}
	defer iRows.Close()

	result := &port.PatientWithInvoices{
		PatientID: patientID,
		FirstName: firstName,
		LastName:  lastName,
	}

	if iRows.Next() {
		iVals, err := iRows.Values()
		if err != nil {
			return nil, err
		}
		if len(iVals) >= 2 {
			if n, ok := iVals[0].(int64); ok {
				result.InvoiceCount = int(n)
			}
			if f, ok := iVals[1].(float64); ok {
				result.TotalBilled = f
			}
		}
	}

	return result, nil
}

func (r *CrossDomainReadRepo) GetAppointmentWithDetails(ctx context.Context, appointmentID uuid.UUID) (*port.AppointmentWithDetails, error) {
	idStr := sqlStr(appointmentID.String())

	// Get appointment info from clinical domain
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	aRows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT patient_id, doctor_id, type, status, scheduled_at FROM appointments WHERE id = %s", idStr))
	if err != nil {
		return nil, err
	}
	defer aRows.Close()
	if !aRows.Next() {
		return nil, fmt.Errorf("appointment not found: %s", appointmentID)
	}
	aVals, err := aRows.Values()
	if err != nil {
		return nil, err
	}

	result := &port.AppointmentWithDetails{
		AppointmentID: appointmentID,
		Type:          fmt.Sprintf("%v", aVals[2]),
		Status:        fmt.Sprintf("%v", aVals[3]),
		ScheduledAt:   parseTS(fmt.Sprintf("%v", aVals[4])),
	}

	patientID := fmt.Sprintf("%v", aVals[0])
	doctorID := fmt.Sprintf("%v", aVals[1])
	aRows.Close()

	// Use IMPORT to get patient name from patients domain
	patImport := fmt.Sprintf(
		"IMPORT patients.patients; SELECT first_name, last_name FROM patients WHERE id = %s",
		sqlStr(patientID),
	)
	pRows, err := r.client.ImportQuery(ctx, domainClinical, patImport)
	if err == nil {
		defer pRows.Close()
		if pRows.Next() {
			pVals, _ := pRows.Values()
			if len(pVals) >= 2 {
				result.PatientName = fmt.Sprintf("%v %v", pVals[0], pVals[1])
			}
		}
		pRows.Close()
	}

	// Use IMPORT to get doctor name from staff domain
	docImport := fmt.Sprintf(
		"IMPORT staff.staff; SELECT first_name, last_name FROM staff WHERE id = %s",
		sqlStr(doctorID),
	)
	dRows, err := r.client.ImportQuery(ctx, domainClinical, docImport)
	if err == nil {
		defer dRows.Close()
		if dRows.Next() {
			dVals, _ := dRows.Values()
			if len(dVals) >= 2 {
				result.DoctorName = fmt.Sprintf("%v %v", dVals[0], dVals[1])
			}
		}
	}

	return result, nil
}

// ── helpers ─────────────────────────────────────────────────────────────────

// scanToMap reads the current row from pgx.Rows into a map[string]string.
func scanToMap(rows pgx.Rows) (map[string]string, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	fds := rows.FieldDescriptions()
	m := make(map[string]string, len(fds))
	for i, fd := range fds {
		if i < len(vals) {
			m[string(fd.Name)] = fmt.Sprintf("%v", vals[i])
		}
	}
	return m, nil
}
