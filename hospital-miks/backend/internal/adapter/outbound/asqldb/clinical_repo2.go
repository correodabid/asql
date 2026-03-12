package asqldb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// ── OperatingRoomRepo ───────────────────────────────────────────────────────

type OperatingRoomRepo struct{ client *Client }

func NewOperatingRoomRepo(c *Client) *OperatingRoomRepo { return &OperatingRoomRepo{client: c} }

func (r *OperatingRoomRepo) Create(ctx context.Context, or *model.OperatingRoom) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO operating_rooms (id, name, code, floor, building, status, equipment, capacity, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %d, %s, %s, %s, %d, %s, %s, %s)`,
		sqlStr(or.ID.String()), sqlStr(or.Name), sqlStr(or.Code),
		or.Floor, sqlStr(or.Building), sqlStr(string(or.Status)),
		sqlStr(or.Equipment), or.Capacity, boolToSQL(or.Active),
		sqlStr(ts(or.CreatedAt)), sqlStr(ts(or.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *OperatingRoomRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.OperatingRoom, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM operating_rooms WHERE id = %s", orCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("operating room not found: %s", id)
	}
	return scanOperatingRoom(rows)
}

func (r *OperatingRoomRepo) Update(ctx context.Context, or *model.OperatingRoom) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE operating_rooms SET name = %s, code = %s, floor = %d, building = %s, status = %s, equipment = %s, capacity = %d, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(or.Name), sqlStr(or.Code), or.Floor, sqlStr(or.Building),
		sqlStr(string(or.Status)), sqlStr(or.Equipment), or.Capacity,
		boolToSQL(or.Active), sqlStr(ts(or.UpdatedAt)), sqlStr(or.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *OperatingRoomRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM operating_rooms WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *OperatingRoomRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.OperatingRoom], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM operating_rooms")
	sql := "SELECT " + orCols + " FROM operating_rooms" + orderClause(f, "name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.OperatingRoom
	for rows.Next() {
		o, err := scanOperatingRoom(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *o)
	}
	return buildListResult(items, total, f), rows.Err()
}

func (r *OperatingRoomRepo) ListAvailable(ctx context.Context, from, to time.Time) ([]model.OperatingRoom, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	// Return all rooms that are active and AVAILABLE status and not booked for surgery in [from, to]
	sql := fmt.Sprintf(
		`SELECT %s FROM operating_rooms WHERE active = true AND status = 'AVAILABLE' AND id NOT IN (SELECT operating_room_id FROM surgeries WHERE status != 'CANCELLED' AND scheduled_start < %s AND scheduled_end > %s) ORDER BY name`,
		orCols, sqlStr(ts(to)), sqlStr(ts(from)),
	)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.OperatingRoom
	for rows.Next() {
		o, err := scanOperatingRoom(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *o)
	}
	return items, rows.Err()
}

const orCols = "id, name, code, floor, building, status, equipment, capacity, active, created_at, updated_at"

func scanOperatingRoom(rows pgx.Rows) (*model.OperatingRoom, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	o := &model.OperatingRoom{}
	if len(vals) >= 11 {
		o.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		o.Name = fmt.Sprintf("%v", vals[1])
		o.Code = fmt.Sprintf("%v", vals[2])
		if n, ok := vals[3].(int64); ok {
			o.Floor = int(n)
		}
		o.Building = fmt.Sprintf("%v", vals[4])
		o.Status = model.OperatingRoomStatus(fmt.Sprintf("%v", vals[5]))
		o.Equipment = fmt.Sprintf("%v", vals[6])
		if n, ok := vals[7].(int64); ok {
			o.Capacity = int(n)
		}
		o.Active = fmt.Sprintf("%v", vals[8]) == "true"
		o.CreatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
		o.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[10]))
	}
	return o, nil
}

// ── SurgeryRepo ─────────────────────────────────────────────────────────────

type SurgeryRepo struct{ client *Client }

func NewSurgeryRepo(c *Client) *SurgeryRepo { return &SurgeryRepo{client: c} }

func (r *SurgeryRepo) Create(ctx context.Context, s *model.Surgery) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO surgeries (id, patient_id, lead_surgeon_id, anesthetist_id, operating_room_id, procedure_name, procedure_code, status, scheduled_start, scheduled_end, actual_start, actual_end, pre_op_notes, post_op_notes, complications, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(s.ID.String()), sqlStr(s.PatientID.String()),
		sqlStr(s.LeadSurgeonID.String()), sqlStr(s.AnesthetistID.String()),
		sqlStr(s.OperatingRoomID.String()), sqlStr(s.ProcedureName),
		sqlStr(s.ProcedureCode), sqlStr(string(s.Status)),
		sqlStr(ts(s.ScheduledStart)), sqlStr(ts(s.ScheduledEnd)),
		nullableTS(s.ActualStart), nullableTS(s.ActualEnd),
		sqlStr(s.PreOpNotes), sqlStr(s.PostOpNotes), sqlStr(s.Complications),
		sqlStr(ts(s.CreatedAt)), sqlStr(ts(s.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *SurgeryRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Surgery, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM surgeries WHERE id = %s", surgeryCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("surgery not found: %s", id)
	}
	return scanSurgery(rows)
}

func (r *SurgeryRepo) Update(ctx context.Context, s *model.Surgery) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE surgeries SET patient_id = %s, lead_surgeon_id = %s, anesthetist_id = %s, operating_room_id = %s, procedure_name = %s, procedure_code = %s, status = %s, scheduled_start = %s, scheduled_end = %s, actual_start = %s, actual_end = %s, pre_op_notes = %s, post_op_notes = %s, complications = %s, updated_at = %s WHERE id = %s`,
		sqlStr(s.PatientID.String()), sqlStr(s.LeadSurgeonID.String()),
		sqlStr(s.AnesthetistID.String()), sqlStr(s.OperatingRoomID.String()),
		sqlStr(s.ProcedureName), sqlStr(s.ProcedureCode),
		sqlStr(string(s.Status)),
		sqlStr(ts(s.ScheduledStart)), sqlStr(ts(s.ScheduledEnd)),
		nullableTS(s.ActualStart), nullableTS(s.ActualEnd),
		sqlStr(s.PreOpNotes), sqlStr(s.PostOpNotes), sqlStr(s.Complications),
		sqlStr(ts(s.UpdatedAt)), sqlStr(s.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *SurgeryRepo) Cancel(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("UPDATE surgeries SET status = 'CANCELLED' WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *SurgeryRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Surgery], error) {
	return r.listWhere(ctx, "", f)
}

func (r *SurgeryRepo) ListByPatient(ctx context.Context, pid uuid.UUID) ([]model.Surgery, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM surgeries WHERE patient_id = %s ORDER BY scheduled_start DESC", surgeryCols, sqlStr(pid.String()))
	return r.scanList(ctx, tx, sql)
}

func (r *SurgeryRepo) ListByDate(ctx context.Context, date time.Time) ([]model.Surgery, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM surgeries WHERE scheduled_start >= %s AND scheduled_start < %s ORDER BY scheduled_start",
		surgeryCols, sqlStr(ts(dayStart)), sqlStr(ts(dayEnd)))
	return r.scanList(ctx, tx, sql)
}

func (r *SurgeryRepo) ListByOperatingRoom(ctx context.Context, orID uuid.UUID, date time.Time) ([]model.Surgery, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM surgeries WHERE operating_room_id = %s AND scheduled_start >= %s AND scheduled_start < %s ORDER BY scheduled_start",
		surgeryCols, sqlStr(orID.String()), sqlStr(ts(dayStart)), sqlStr(ts(dayEnd)))
	return r.scanList(ctx, tx, sql)
}

func (r *SurgeryRepo) AddTeamMember(ctx context.Context, m *model.SurgeryTeamMember) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO surgery_team_members (id, surgery_id, staff_id, role, created_at)
		 VALUES (%s, %s, %s, %s, %s)`,
		sqlStr(m.ID.String()), sqlStr(m.SurgeryID.String()),
		sqlStr(m.StaffID.String()), sqlStr(m.Role), sqlStr(ts(m.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *SurgeryRepo) GetTeamMembers(ctx context.Context, surgeryID uuid.UUID) ([]model.SurgeryTeamMember, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, surgery_id, staff_id, role, created_at FROM surgery_team_members WHERE surgery_id = %s ORDER BY created_at", sqlStr(surgeryID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.SurgeryTeamMember
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		m := model.SurgeryTeamMember{}
		if len(vals) >= 5 {
			m.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
			m.SurgeryID = parseUUID(fmt.Sprintf("%v", vals[1]))
			m.StaffID = parseUUID(fmt.Sprintf("%v", vals[2]))
			m.Role = fmt.Sprintf("%v", vals[3])
			m.CreatedAt = parseTS(fmt.Sprintf("%v", vals[4]))
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

func (r *SurgeryRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Surgery], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM surgeries "+where)
	sql := "SELECT " + surgeryCols + " FROM surgeries " + where + orderClause(f, "scheduled_start") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Surgery
	for rows.Next() {
		s, err := scanSurgery(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return buildListResult(items, total, f), rows.Err()
}

func (r *SurgeryRepo) scanList(ctx context.Context, tx *DomainTx, sql string) ([]model.Surgery, error) {
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Surgery
	for rows.Next() {
		s, err := scanSurgery(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return items, rows.Err()
}

const surgeryCols = "id, patient_id, lead_surgeon_id, anesthetist_id, operating_room_id, procedure_name, procedure_code, status, scheduled_start, scheduled_end, actual_start, actual_end, pre_op_notes, post_op_notes, complications, created_at, updated_at"

func scanSurgery(rows pgx.Rows) (*model.Surgery, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	s := &model.Surgery{}
	if len(vals) >= 17 {
		s.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		s.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		s.LeadSurgeonID = parseUUID(fmt.Sprintf("%v", vals[2]))
		s.AnesthetistID = parseUUID(fmt.Sprintf("%v", vals[3]))
		s.OperatingRoomID = parseUUID(fmt.Sprintf("%v", vals[4]))
		s.ProcedureName = fmt.Sprintf("%v", vals[5])
		s.ProcedureCode = fmt.Sprintf("%v", vals[6])
		s.Status = model.SurgeryStatus(fmt.Sprintf("%v", vals[7]))
		s.ScheduledStart = parseTS(fmt.Sprintf("%v", vals[8]))
		s.ScheduledEnd = parseTS(fmt.Sprintf("%v", vals[9]))
		if v := fmt.Sprintf("%v", vals[10]); v != "" && v != "<nil>" {
			t := parseTS(v)
			s.ActualStart = &t
		}
		if v := fmt.Sprintf("%v", vals[11]); v != "" && v != "<nil>" {
			t := parseTS(v)
			s.ActualEnd = &t
		}
		s.PreOpNotes = fmt.Sprintf("%v", vals[12])
		s.PostOpNotes = fmt.Sprintf("%v", vals[13])
		s.Complications = fmt.Sprintf("%v", vals[14])
		s.CreatedAt = parseTS(fmt.Sprintf("%v", vals[15]))
		s.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[16]))
	}
	return s, nil
}

// ── AdmissionRepo ───────────────────────────────────────────────────────────

type AdmissionRepo struct{ client *Client }

func NewAdmissionRepo(c *Client) *AdmissionRepo { return &AdmissionRepo{client: c} }

func (r *AdmissionRepo) Create(ctx context.Context, a *model.Admission) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO admissions (id, patient_id, admitting_doctor_id, bed_id, department_id, status, admission_date, discharge_date, diagnosis, admission_reason, dietary_needs, notes, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(a.ID.String()), sqlStr(a.PatientID.String()),
		sqlStr(a.AdmittingDocID.String()), sqlStr(a.BedID.String()),
		sqlStr(a.DepartmentID.String()), sqlStr(string(a.Status)),
		sqlStr(ts(a.AdmissionDate)), nullableTS(a.DischargeDate),
		sqlStr(a.Diagnosis), sqlStr(a.AdmissionReason),
		sqlStr(a.DietaryNeeds), sqlStr(a.Notes),
		sqlStr(ts(a.CreatedAt)), sqlStr(ts(a.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *AdmissionRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Admission, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM admissions WHERE id = %s", admCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("admission not found: %s", id)
	}
	return scanAdmission(rows)
}

func (r *AdmissionRepo) Update(ctx context.Context, a *model.Admission) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE admissions SET patient_id = %s, admitting_doctor_id = %s, bed_id = %s, department_id = %s, status = %s, admission_date = %s, discharge_date = %s, diagnosis = %s, admission_reason = %s, dietary_needs = %s, notes = %s, updated_at = %s WHERE id = %s`,
		sqlStr(a.PatientID.String()), sqlStr(a.AdmittingDocID.String()),
		sqlStr(a.BedID.String()), sqlStr(a.DepartmentID.String()),
		sqlStr(string(a.Status)), sqlStr(ts(a.AdmissionDate)),
		nullableTS(a.DischargeDate),
		sqlStr(a.Diagnosis), sqlStr(a.AdmissionReason),
		sqlStr(a.DietaryNeeds), sqlStr(a.Notes),
		sqlStr(ts(a.UpdatedAt)), sqlStr(a.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *AdmissionRepo) Discharge(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	sql := fmt.Sprintf("UPDATE admissions SET status = 'DISCHARGED', discharge_date = %s, updated_at = %s WHERE id = %s",
		sqlStr(ts(now)), sqlStr(ts(now)), sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *AdmissionRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Admission], error) {
	return r.listWhere(ctx, "", f)
}

func (r *AdmissionRepo) ListByPatient(ctx context.Context, pid uuid.UUID) ([]model.Admission, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM admissions WHERE patient_id = %s ORDER BY admission_date DESC", admCols, sqlStr(pid.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Admission
	for rows.Next() {
		a, err := scanAdmission(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *a)
	}
	return items, rows.Err()
}

func (r *AdmissionRepo) ListActive(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Admission], error) {
	return r.listWhere(ctx, "WHERE status IN ('ADMITTED', 'IN_CARE')", f)
}

func (r *AdmissionRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Admission], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM admissions "+where)
	sql := "SELECT " + admCols + " FROM admissions " + where + orderClause(f, "admission_date") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Admission
	for rows.Next() {
		a, err := scanAdmission(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *a)
	}
	return buildListResult(items, total, f), rows.Err()
}

const admCols = "id, patient_id, admitting_doctor_id, bed_id, department_id, status, admission_date, discharge_date, diagnosis, admission_reason, dietary_needs, notes, created_at, updated_at"

func scanAdmission(rows pgx.Rows) (*model.Admission, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	a := &model.Admission{}
	if len(vals) >= 14 {
		a.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		a.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		a.AdmittingDocID = parseUUID(fmt.Sprintf("%v", vals[2]))
		a.BedID = parseUUID(fmt.Sprintf("%v", vals[3]))
		a.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[4]))
		a.Status = model.AdmissionStatus(fmt.Sprintf("%v", vals[5]))
		a.AdmissionDate = parseTS(fmt.Sprintf("%v", vals[6]))
		if v := fmt.Sprintf("%v", vals[7]); v != "" && v != "<nil>" {
			t := parseTS(v)
			a.DischargeDate = &t
		}
		a.Diagnosis = fmt.Sprintf("%v", vals[8])
		a.AdmissionReason = fmt.Sprintf("%v", vals[9])
		a.DietaryNeeds = fmt.Sprintf("%v", vals[10])
		a.Notes = fmt.Sprintf("%v", vals[11])
		a.CreatedAt = parseTS(fmt.Sprintf("%v", vals[12]))
		a.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[13]))
	}
	return a, nil
}

// ── WardRepo ────────────────────────────────────────────────────────────────

type WardRepo struct{ client *Client }

func NewWardRepo(c *Client) *WardRepo { return &WardRepo{client: c} }

func (r *WardRepo) Create(ctx context.Context, w *model.Ward) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO wards (id, name, code, department_id, floor, building, total_beds, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %d, %s, %d, %s, %s, %s)`,
		sqlStr(w.ID.String()), sqlStr(w.Name), sqlStr(w.Code),
		sqlStr(w.DepartmentID.String()), w.Floor, sqlStr(w.Building),
		w.TotalBeds, boolToSQL(w.Active),
		sqlStr(ts(w.CreatedAt)), sqlStr(ts(w.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *WardRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Ward, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM wards WHERE id = %s", wardCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("ward not found: %s", id)
	}
	return scanWard(rows)
}

func (r *WardRepo) Update(ctx context.Context, w *model.Ward) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE wards SET name = %s, code = %s, department_id = %s, floor = %d, building = %s, total_beds = %d, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(w.Name), sqlStr(w.Code), sqlStr(w.DepartmentID.String()),
		w.Floor, sqlStr(w.Building), w.TotalBeds, boolToSQL(w.Active),
		sqlStr(ts(w.UpdatedAt)), sqlStr(w.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *WardRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM wards WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *WardRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Ward], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM wards")
	sql := "SELECT " + wardCols + " FROM wards" + orderClause(f, "name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Ward
	for rows.Next() {
		w, err := scanWard(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *w)
	}
	return buildListResult(items, total, f), rows.Err()
}

const wardCols = "id, name, code, department_id, floor, building, total_beds, active, created_at, updated_at"

func scanWard(rows pgx.Rows) (*model.Ward, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	w := &model.Ward{}
	if len(vals) >= 10 {
		w.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		w.Name = fmt.Sprintf("%v", vals[1])
		w.Code = fmt.Sprintf("%v", vals[2])
		w.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[3]))
		if n, ok := vals[4].(int64); ok {
			w.Floor = int(n)
		}
		w.Building = fmt.Sprintf("%v", vals[5])
		if n, ok := vals[6].(int64); ok {
			w.TotalBeds = int(n)
		}
		w.Active = fmt.Sprintf("%v", vals[7]) == "true"
		w.CreatedAt = parseTS(fmt.Sprintf("%v", vals[8]))
		w.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
	}
	return w, nil
}

// ── BedRepo ─────────────────────────────────────────────────────────────────

type BedRepo struct{ client *Client }

func NewBedRepo(c *Client) *BedRepo { return &BedRepo{client: c} }

func (r *BedRepo) Create(ctx context.Context, b *model.Bed) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO beds (id, ward_id, number, status, room_no, features, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(b.ID.String()), sqlStr(b.WardID.String()),
		sqlStr(b.Number), sqlStr(string(b.Status)),
		sqlStr(b.RoomNo), sqlStr(b.Features),
		sqlStr(ts(b.CreatedAt)), sqlStr(ts(b.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *BedRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Bed, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM beds WHERE id = %s", bedCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("bed not found: %s", id)
	}
	return scanBed(rows)
}

func (r *BedRepo) Update(ctx context.Context, b *model.Bed) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE beds SET ward_id = %s, number = %s, status = %s, room_no = %s, features = %s, updated_at = %s WHERE id = %s`,
		sqlStr(b.WardID.String()), sqlStr(b.Number), sqlStr(string(b.Status)),
		sqlStr(b.RoomNo), sqlStr(b.Features), sqlStr(ts(b.UpdatedAt)), sqlStr(b.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *BedRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM beds WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *BedRepo) ListByWard(ctx context.Context, wardID uuid.UUID) ([]model.Bed, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM beds WHERE ward_id = %s ORDER BY number", bedCols, sqlStr(wardID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Bed
	for rows.Next() {
		b, err := scanBed(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *b)
	}
	return items, rows.Err()
}

func (r *BedRepo) ListAvailable(ctx context.Context) ([]model.Bed, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM beds WHERE status = 'AVAILABLE' ORDER BY ward_id, number", bedCols)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Bed
	for rows.Next() {
		b, err := scanBed(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *b)
	}
	return items, rows.Err()
}

const bedCols = "id, ward_id, number, status, room_no, features, created_at, updated_at"

func scanBed(rows pgx.Rows) (*model.Bed, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	b := &model.Bed{}
	if len(vals) >= 8 {
		b.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		b.WardID = parseUUID(fmt.Sprintf("%v", vals[1]))
		b.Number = fmt.Sprintf("%v", vals[2])
		b.Status = model.BedStatus(fmt.Sprintf("%v", vals[3]))
		b.RoomNo = fmt.Sprintf("%v", vals[4])
		b.Features = fmt.Sprintf("%v", vals[5])
		b.CreatedAt = parseTS(fmt.Sprintf("%v", vals[6]))
		b.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[7]))
	}
	return b, nil
}

// ── MealOrderRepo ──────────────────────────────────────────────────────────

type MealOrderRepo struct{ client *Client }

func NewMealOrderRepo(c *Client) *MealOrderRepo { return &MealOrderRepo{client: c} }

func (r *MealOrderRepo) Create(ctx context.Context, m *model.MealOrder) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO meal_orders (id, admission_id, meal_type, date, menu, dietary_note, delivered, delivered_at, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(m.ID.String()), sqlStr(m.AdmissionID.String()),
		sqlStr(string(m.MealType)), sqlStr(ts(m.Date)),
		sqlStr(m.Menu), sqlStr(m.DietaryNote),
		boolToSQL(m.Delivered), nullableTS(m.DeliveredAt),
		sqlStr(ts(m.CreatedAt)), sqlStr(ts(m.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MealOrderRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.MealOrder, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM meal_orders WHERE id = %s", mealCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("meal order not found: %s", id)
	}
	return scanMealOrder(rows)
}

func (r *MealOrderRepo) Update(ctx context.Context, m *model.MealOrder) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE meal_orders SET meal_type = %s, date = %s, menu = %s, dietary_note = %s, delivered = %s, delivered_at = %s, updated_at = %s WHERE id = %s`,
		sqlStr(string(m.MealType)), sqlStr(ts(m.Date)),
		sqlStr(m.Menu), sqlStr(m.DietaryNote),
		boolToSQL(m.Delivered), nullableTS(m.DeliveredAt),
		sqlStr(ts(m.UpdatedAt)), sqlStr(m.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MealOrderRepo) ListByAdmission(ctx context.Context, admID uuid.UUID) ([]model.MealOrder, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM meal_orders WHERE admission_id = %s ORDER BY date DESC, meal_type", mealCols, sqlStr(admID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.MealOrder
	for rows.Next() {
		m, err := scanMealOrder(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

const mealCols = "id, admission_id, meal_type, date, menu, dietary_note, delivered, delivered_at, created_at, updated_at"

func scanMealOrder(rows pgx.Rows) (*model.MealOrder, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	m := &model.MealOrder{}
	if len(vals) >= 10 {
		m.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		m.AdmissionID = parseUUID(fmt.Sprintf("%v", vals[1]))
		m.MealType = model.MealType(fmt.Sprintf("%v", vals[2]))
		m.Date = parseTS(fmt.Sprintf("%v", vals[3]))
		m.Menu = fmt.Sprintf("%v", vals[4])
		m.DietaryNote = fmt.Sprintf("%v", vals[5])
		m.Delivered = fmt.Sprintf("%v", vals[6]) == "true"
		if v := fmt.Sprintf("%v", vals[7]); v != "" && v != "<nil>" {
			t := parseTS(v)
			m.DeliveredAt = &t
		}
		m.CreatedAt = parseTS(fmt.Sprintf("%v", vals[8]))
		m.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
	}
	return m, nil
}

// ── CareNoteRepo ───────────────────────────────────────────────────────────

type CareNoteRepo struct{ client *Client }

func NewCareNoteRepo(c *Client) *CareNoteRepo { return &CareNoteRepo{client: c} }

func (r *CareNoteRepo) Create(ctx context.Context, n *model.CareNote) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO care_notes (id, admission_id, staff_id, note_type, content, created_at)
		 VALUES (%s, %s, %s, %s, %s, %s)`,
		sqlStr(n.ID.String()), sqlStr(n.AdmissionID.String()),
		sqlStr(n.StaffID.String()), sqlStr(n.NoteType),
		sqlStr(n.Content), sqlStr(ts(n.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *CareNoteRepo) ListByAdmission(ctx context.Context, admID uuid.UUID) ([]model.CareNote, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, admission_id, staff_id, note_type, content, created_at FROM care_notes WHERE admission_id = %s ORDER BY created_at DESC",
		sqlStr(admID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.CareNote
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		n := model.CareNote{}
		if len(vals) >= 6 {
			n.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
			n.AdmissionID = parseUUID(fmt.Sprintf("%v", vals[1]))
			n.StaffID = parseUUID(fmt.Sprintf("%v", vals[2]))
			n.NoteType = fmt.Sprintf("%v", vals[3])
			n.Content = fmt.Sprintf("%v", vals[4])
			n.CreatedAt = parseTS(fmt.Sprintf("%v", vals[5]))
		}
		items = append(items, n)
	}
	return items, rows.Err()
}
