package asqldb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainClinical = "clinical"

// ── AppointmentRepo ─────────────────────────────────────────────────────────

type AppointmentRepo struct{ client *Client }

func NewAppointmentRepo(c *Client) *AppointmentRepo { return &AppointmentRepo{client: c} }

func (r *AppointmentRepo) Create(ctx context.Context, a *model.Appointment) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO appointments (id, patient_id, doctor_id, department_id, type, status, scheduled_at, duration_minutes, room, notes, diagnosis, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s)`,
		sqlStr(a.ID.String()), sqlStr(a.PatientID.String()), sqlStr(a.DoctorID.String()),
		sqlStr(a.DepartmentID.String()),
		sqlStr(string(a.Type)), sqlStr(string(a.Status)),
		sqlStr(ts(a.ScheduledAt)), a.Duration,
		sqlStr(a.Room), sqlStr(a.Notes), sqlStr(a.Diagnosis),
		sqlStr(ts(a.CreatedAt)), sqlStr(ts(a.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("appointment create: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *AppointmentRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Appointment, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM appointments WHERE id = %s", apptCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("appointment not found: %s", id)
	}
	return scanAppointment(rows)
}

func (r *AppointmentRepo) Update(ctx context.Context, a *model.Appointment) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE appointments SET patient_id = %s, doctor_id = %s, department_id = %s, type = %s, status = %s, scheduled_at = %s, duration_minutes = %d, room = %s, notes = %s, diagnosis = %s, updated_at = %s WHERE id = %s`,
		sqlStr(a.PatientID.String()), sqlStr(a.DoctorID.String()),
		sqlStr(a.DepartmentID.String()),
		sqlStr(string(a.Type)), sqlStr(string(a.Status)),
		sqlStr(ts(a.ScheduledAt)), a.Duration,
		sqlStr(a.Room), sqlStr(a.Notes), sqlStr(a.Diagnosis),
		sqlStr(ts(a.UpdatedAt)), sqlStr(a.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("appointment update: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *AppointmentRepo) Cancel(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("UPDATE appointments SET status = 'CANCELLED' WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *AppointmentRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Appointment], error) {
	return r.listWhere(ctx, "", f)
}

func (r *AppointmentRepo) ListByPatient(ctx context.Context, pid uuid.UUID, f model.ListFilter) (*model.ListResult[model.Appointment], error) {
	return r.listWhere(ctx, fmt.Sprintf("WHERE patient_id = %s", sqlStr(pid.String())), f)
}

func (r *AppointmentRepo) ListByDoctor(ctx context.Context, did uuid.UUID, f model.ListFilter) (*model.ListResult[model.Appointment], error) {
	return r.listWhere(ctx, fmt.Sprintf("WHERE doctor_id = %s", sqlStr(did.String())), f)
}

func (r *AppointmentRepo) ListByDateRange(ctx context.Context, from, to time.Time, f model.ListFilter) (*model.ListResult[model.Appointment], error) {
	where := fmt.Sprintf("WHERE scheduled_at >= %s AND scheduled_at <= %s", sqlStr(ts(from)), sqlStr(ts(to)))
	return r.listWhere(ctx, where, f)
}

func (r *AppointmentRepo) ListByDoctorAndDate(ctx context.Context, did uuid.UUID, date time.Time) ([]model.Appointment, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	where := fmt.Sprintf("WHERE doctor_id = %s AND scheduled_at >= %s AND scheduled_at < %s",
		sqlStr(did.String()), sqlStr(ts(dayStart)), sqlStr(ts(dayEnd)))

	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := "SELECT " + apptCols + " FROM appointments " + where + " ORDER BY scheduled_at ASC"
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Appointment
	for rows.Next() {
		a, err := scanAppointment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *a)
	}
	return items, rows.Err()
}

func (r *AppointmentRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Appointment], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM appointments "+where)
	sql := "SELECT " + apptCols + " FROM appointments " + where + orderClause(f, "scheduled_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Appointment
	for rows.Next() {
		a, err := scanAppointment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *a)
	}
	return buildListResult(items, total, f), rows.Err()
}

const apptCols = "id, patient_id, doctor_id, department_id, type, status, scheduled_at, duration_minutes, room, notes, diagnosis, created_at, updated_at"

func scanAppointment(rows pgx.Rows) (*model.Appointment, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	a := &model.Appointment{}
	if len(vals) >= 13 {
		a.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		a.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		a.DoctorID = parseUUID(fmt.Sprintf("%v", vals[2]))
		a.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[3]))
		a.Type = model.AppointmentType(fmt.Sprintf("%v", vals[4]))
		a.Status = model.AppointmentStatus(fmt.Sprintf("%v", vals[5]))
		a.ScheduledAt = parseTS(fmt.Sprintf("%v", vals[6]))
		if n, ok := vals[7].(int64); ok {
			a.Duration = int(n)
		}
		a.Room = fmt.Sprintf("%v", vals[8])
		a.Notes = fmt.Sprintf("%v", vals[9])
		a.Diagnosis = fmt.Sprintf("%v", vals[10])
		a.CreatedAt = parseTS(fmt.Sprintf("%v", vals[11]))
		a.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[12]))
	}
	return a, nil
}

// ── ConsultationRoomRepo ────────────────────────────────────────────────────

type ConsultationRoomRepo struct{ client *Client }

func NewConsultationRoomRepo(c *Client) *ConsultationRoomRepo {
	return &ConsultationRoomRepo{client: c}
}

func (r *ConsultationRoomRepo) Create(ctx context.Context, room *model.ConsultationRoom) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO consultation_rooms (id, name, code, department_id, floor, building, equipment, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %d, %s, %s, %s, %s, %s)`,
		sqlStr(room.ID.String()), sqlStr(room.Name), sqlStr(room.Code),
		sqlStr(room.DepartmentID.String()), room.Floor, sqlStr(room.Building),
		sqlStr(room.Equipment), boolToSQL(room.Active),
		sqlStr(ts(room.CreatedAt)), sqlStr(ts(room.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *ConsultationRoomRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.ConsultationRoom, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM consultation_rooms WHERE id = %s", roomCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("consultation room not found: %s", id)
	}
	return scanConsultationRoom(rows)
}

func (r *ConsultationRoomRepo) Update(ctx context.Context, room *model.ConsultationRoom) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE consultation_rooms SET name = %s, code = %s, department_id = %s, floor = %d, building = %s, equipment = %s, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(room.Name), sqlStr(room.Code), sqlStr(room.DepartmentID.String()),
		room.Floor, sqlStr(room.Building), sqlStr(room.Equipment),
		boolToSQL(room.Active), sqlStr(ts(room.UpdatedAt)), sqlStr(room.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *ConsultationRoomRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM consultation_rooms WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *ConsultationRoomRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.ConsultationRoom], error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM consultation_rooms")
	sql := "SELECT " + roomCols + " FROM consultation_rooms" + orderClause(f, "name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.ConsultationRoom
	for rows.Next() {
		cr, err := scanConsultationRoom(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *cr)
	}
	return buildListResult(items, total, f), rows.Err()
}

func (r *ConsultationRoomRepo) ListByDepartment(ctx context.Context, deptID uuid.UUID) ([]model.ConsultationRoom, error) {
	tx, err := r.client.BeginDomain(ctx, domainClinical)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM consultation_rooms WHERE department_id = %s ORDER BY name ASC", roomCols, sqlStr(deptID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.ConsultationRoom
	for rows.Next() {
		cr, err := scanConsultationRoom(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *cr)
	}
	return items, rows.Err()
}

const roomCols = "id, name, code, department_id, floor, building, equipment, active, created_at, updated_at"

func scanConsultationRoom(rows pgx.Rows) (*model.ConsultationRoom, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	cr := &model.ConsultationRoom{}
	if len(vals) >= 10 {
		cr.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		cr.Name = fmt.Sprintf("%v", vals[1])
		cr.Code = fmt.Sprintf("%v", vals[2])
		cr.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[3]))
		if n, ok := vals[4].(int64); ok {
			cr.Floor = int(n)
		}
		cr.Building = fmt.Sprintf("%v", vals[5])
		cr.Equipment = fmt.Sprintf("%v", vals[6])
		cr.Active = fmt.Sprintf("%v", vals[7]) == "true"
		cr.CreatedAt = parseTS(fmt.Sprintf("%v", vals[8]))
		cr.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
	}
	return cr, nil
}
