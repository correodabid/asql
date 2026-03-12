package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainRehab = "rehab"

// ── RehabPlanRepo ───────────────────────────────────────────────────────────

type RehabPlanRepo struct{ client *Client }

func NewRehabPlanRepo(c *Client) *RehabPlanRepo { return &RehabPlanRepo{client: c} }

func (r *RehabPlanRepo) Create(ctx context.Context, p *model.RehabPlan) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO rehab_plans (id, patient_id, therapist_id, doctor_id, type, diagnosis, goals, start_date, end_date, total_sessions, completed_sessions, active, notes, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %s, %s, %s, %s)`,
		sqlStr(p.ID.String()), sqlStr(p.PatientID.String()),
		sqlStr(p.TherapistID.String()), sqlStr(p.DoctorID.String()),
		sqlStr(string(p.Type)), sqlStr(p.Diagnosis), sqlStr(p.Goals),
		sqlStr(ts(p.StartDate)), nullableTS(p.EndDate),
		p.Sessions, p.Completed, boolToSQL(p.Active), sqlStr(p.Notes),
		sqlStr(ts(p.CreatedAt)), sqlStr(ts(p.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabPlanRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.RehabPlan, error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM rehab_plans WHERE id = %s", planCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("rehab plan not found: %s", id)
	}
	return scanPlan(rows)
}

func (r *RehabPlanRepo) Update(ctx context.Context, p *model.RehabPlan) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE rehab_plans SET patient_id = %s, therapist_id = %s, doctor_id = %s, type = %s, diagnosis = %s, goals = %s, start_date = %s, end_date = %s, total_sessions = %d, completed_sessions = %d, active = %s, notes = %s, updated_at = %s WHERE id = %s`,
		sqlStr(p.PatientID.String()), sqlStr(p.TherapistID.String()),
		sqlStr(p.DoctorID.String()), sqlStr(string(p.Type)),
		sqlStr(p.Diagnosis), sqlStr(p.Goals),
		sqlStr(ts(p.StartDate)), nullableTS(p.EndDate),
		p.Sessions, p.Completed, boolToSQL(p.Active), sqlStr(p.Notes),
		sqlStr(ts(p.UpdatedAt)), sqlStr(p.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabPlanRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM rehab_plans WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabPlanRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.RehabPlan], error) {
	return r.listWhere(ctx, "", f)
}

func (r *RehabPlanRepo) ListByPatient(ctx context.Context, pid uuid.UUID) ([]model.RehabPlan, error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM rehab_plans WHERE patient_id = %s ORDER BY start_date DESC", planCols, sqlStr(pid.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.RehabPlan
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *p)
	}
	return items, rows.Err()
}

func (r *RehabPlanRepo) ListActive(ctx context.Context, f model.ListFilter) (*model.ListResult[model.RehabPlan], error) {
	return r.listWhere(ctx, "WHERE active = true", f)
}

func (r *RehabPlanRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.RehabPlan], error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM rehab_plans "+where)
	sql := "SELECT " + planCols + " FROM rehab_plans " + where + orderClause(f, "start_date") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.RehabPlan
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *p)
	}
	return buildListResult(items, total, f), rows.Err()
}

const planCols = "id, patient_id, therapist_id, doctor_id, type, diagnosis, goals, start_date, end_date, total_sessions, completed_sessions, active, notes, created_at, updated_at"

func scanPlan(rows pgx.Rows) (*model.RehabPlan, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	p := &model.RehabPlan{}
	if len(vals) >= 15 {
		p.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		p.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		p.TherapistID = parseUUID(fmt.Sprintf("%v", vals[2]))
		p.DoctorID = parseUUID(fmt.Sprintf("%v", vals[3]))
		p.Type = model.TherapyType(fmt.Sprintf("%v", vals[4]))
		p.Diagnosis = fmt.Sprintf("%v", vals[5])
		p.Goals = fmt.Sprintf("%v", vals[6])
		p.StartDate = parseTS(fmt.Sprintf("%v", vals[7]))
		if v := fmt.Sprintf("%v", vals[8]); v != "" && v != "<nil>" {
			t := parseTS(v)
			p.EndDate = &t
		}
		if n, ok := vals[9].(int64); ok {
			p.Sessions = int(n)
		}
		if n, ok := vals[10].(int64); ok {
			p.Completed = int(n)
		}
		p.Active = fmt.Sprintf("%v", vals[11]) == "true"
		p.Notes = fmt.Sprintf("%v", vals[12])
		p.CreatedAt = parseTS(fmt.Sprintf("%v", vals[13]))
		p.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[14]))
	}
	return p, nil
}

// ── RehabSessionRepo ────────────────────────────────────────────────────────

type RehabSessionRepo struct{ client *Client }

func NewRehabSessionRepo(c *Client) *RehabSessionRepo { return &RehabSessionRepo{client: c} }

func (r *RehabSessionRepo) Create(ctx context.Context, s *model.RehabSession) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	painLevel := "NULL"
	if s.PainLevel != nil {
		painLevel = fmt.Sprintf("%d", *s.PainLevel)
	}
	sql := fmt.Sprintf(
		`INSERT INTO rehab_sessions (id, plan_id, therapist_id, patient_id, status, scheduled_at, duration_minutes, room, exercises, progress, pain_level, notes, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(s.ID.String()), sqlStr(s.PlanID.String()),
		sqlStr(s.TherapistID.String()), sqlStr(s.PatientID.String()),
		sqlStr(string(s.Status)), sqlStr(ts(s.ScheduledAt)),
		s.Duration, sqlStr(s.Room),
		sqlStr(s.Exercises), sqlStr(s.Progress),
		painLevel, sqlStr(s.Notes),
		sqlStr(ts(s.CreatedAt)), sqlStr(ts(s.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabSessionRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.RehabSession, error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM rehab_sessions WHERE id = %s", sessCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("rehab session not found: %s", id)
	}
	return scanSession(rows)
}

func (r *RehabSessionRepo) Update(ctx context.Context, s *model.RehabSession) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	painLevel := "NULL"
	if s.PainLevel != nil {
		painLevel = fmt.Sprintf("%d", *s.PainLevel)
	}
	sql := fmt.Sprintf(
		`UPDATE rehab_sessions SET plan_id = %s, therapist_id = %s, patient_id = %s, status = %s, scheduled_at = %s, duration_minutes = %d, room = %s, exercises = %s, progress = %s, pain_level = %s, notes = %s, updated_at = %s WHERE id = %s`,
		sqlStr(s.PlanID.String()), sqlStr(s.TherapistID.String()),
		sqlStr(s.PatientID.String()), sqlStr(string(s.Status)),
		sqlStr(ts(s.ScheduledAt)), s.Duration,
		sqlStr(s.Room), sqlStr(s.Exercises), sqlStr(s.Progress),
		painLevel, sqlStr(s.Notes),
		sqlStr(ts(s.UpdatedAt)), sqlStr(s.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabSessionRepo) Cancel(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("UPDATE rehab_sessions SET status = 'CANCELLED' WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *RehabSessionRepo) ListByPlan(ctx context.Context, planID uuid.UUID) ([]model.RehabSession, error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM rehab_sessions WHERE plan_id = %s ORDER BY scheduled_at", sessCols, sqlStr(planID.String()))
	return r.scanAll(ctx, tx, sql)
}

func (r *RehabSessionRepo) ListByTherapist(ctx context.Context, therapistID uuid.UUID, f model.ListFilter) (*model.ListResult[model.RehabSession], error) {
	where := fmt.Sprintf("WHERE therapist_id = %s", sqlStr(therapistID.String()))
	return r.listWhere(ctx, where, f)
}

func (r *RehabSessionRepo) ListByPatient(ctx context.Context, pid uuid.UUID, f model.ListFilter) (*model.ListResult[model.RehabSession], error) {
	where := fmt.Sprintf("WHERE patient_id = %s", sqlStr(pid.String()))
	return r.listWhere(ctx, where, f)
}

func (r *RehabSessionRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.RehabSession], error) {
	tx, err := r.client.BeginDomain(ctx, domainRehab)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM rehab_sessions "+where)
	sql := "SELECT " + sessCols + " FROM rehab_sessions " + where + orderClause(f, "scheduled_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.RehabSession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return buildListResult(items, total, f), rows.Err()
}

func (r *RehabSessionRepo) scanAll(ctx context.Context, tx *DomainTx, sql string) ([]model.RehabSession, error) {
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.RehabSession
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return items, rows.Err()
}

const sessCols = "id, plan_id, therapist_id, patient_id, status, scheduled_at, duration_minutes, room, exercises, progress, pain_level, notes, created_at, updated_at"

func scanSession(rows pgx.Rows) (*model.RehabSession, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	s := &model.RehabSession{}
	if len(vals) >= 14 {
		s.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		s.PlanID = parseUUID(fmt.Sprintf("%v", vals[1]))
		s.TherapistID = parseUUID(fmt.Sprintf("%v", vals[2]))
		s.PatientID = parseUUID(fmt.Sprintf("%v", vals[3]))
		s.Status = model.SessionStatus(fmt.Sprintf("%v", vals[4]))
		s.ScheduledAt = parseTS(fmt.Sprintf("%v", vals[5]))
		if n, ok := vals[6].(int64); ok {
			s.Duration = int(n)
		}
		s.Room = fmt.Sprintf("%v", vals[7])
		s.Exercises = fmt.Sprintf("%v", vals[8])
		s.Progress = fmt.Sprintf("%v", vals[9])
		if v := fmt.Sprintf("%v", vals[10]); v != "" && v != "<nil>" {
			if n, ok := vals[10].(int64); ok {
				level := int(n)
				s.PainLevel = &level
			}
		}
		s.Notes = fmt.Sprintf("%v", vals[11])
		s.CreatedAt = parseTS(fmt.Sprintf("%v", vals[12]))
		s.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[13]))
	}
	return s, nil
}
