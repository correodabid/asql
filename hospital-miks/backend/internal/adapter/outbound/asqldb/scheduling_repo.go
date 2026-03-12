package asqldb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainScheduling = "scheduling"

type GuardShiftRepo struct{ client *Client }

func NewGuardShiftRepo(c *Client) *GuardShiftRepo { return &GuardShiftRepo{client: c} }

func (r *GuardShiftRepo) Create(ctx context.Context, s *model.GuardShift) error {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return err
	}
	swapped := "NULL"
	if s.SwappedWith != nil {
		swapped = sqlStr(s.SwappedWith.String())
	}
	sql := fmt.Sprintf(
		`INSERT INTO guard_shifts (id, staff_id, department_id, type, status, start_time, end_time, notes, swapped_with, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(s.ID.String()), sqlStr(s.StaffID.String()),
		sqlStr(s.DepartmentID.String()),
		sqlStr(string(s.Type)), sqlStr(string(s.Status)),
		sqlStr(ts(s.StartTime)), sqlStr(ts(s.EndTime)),
		sqlStr(s.Notes), swapped,
		sqlStr(ts(s.CreatedAt)), sqlStr(ts(s.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *GuardShiftRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.GuardShift, error) {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM guard_shifts WHERE id = %s", shiftCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("guard shift not found: %s", id)
	}
	return scanShift(rows)
}

func (r *GuardShiftRepo) Update(ctx context.Context, s *model.GuardShift) error {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return err
	}
	swapped := "NULL"
	if s.SwappedWith != nil {
		swapped = sqlStr(s.SwappedWith.String())
	}
	sql := fmt.Sprintf(
		`UPDATE guard_shifts SET staff_id = %s, department_id = %s, type = %s, status = %s, start_time = %s, end_time = %s, notes = %s, swapped_with = %s, updated_at = %s WHERE id = %s`,
		sqlStr(s.StaffID.String()), sqlStr(s.DepartmentID.String()),
		sqlStr(string(s.Type)), sqlStr(string(s.Status)),
		sqlStr(ts(s.StartTime)), sqlStr(ts(s.EndTime)),
		sqlStr(s.Notes), swapped,
		sqlStr(ts(s.UpdatedAt)), sqlStr(s.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *GuardShiftRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM guard_shifts WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *GuardShiftRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.GuardShift], error) {
	return r.listWhere(ctx, "", f)
}

func (r *GuardShiftRepo) ListByStaff(ctx context.Context, staffID uuid.UUID, from, to time.Time) ([]model.GuardShift, error) {
	where := fmt.Sprintf("WHERE staff_id = %s AND start_time >= %s AND end_time <= %s",
		sqlStr(staffID.String()), sqlStr(ts(from)), sqlStr(ts(to)))
	return r.plainList(ctx, where)
}

func (r *GuardShiftRepo) ListByDepartment(ctx context.Context, deptID uuid.UUID, from, to time.Time) ([]model.GuardShift, error) {
	where := fmt.Sprintf("WHERE department_id = %s AND start_time >= %s AND end_time <= %s",
		sqlStr(deptID.String()), sqlStr(ts(from)), sqlStr(ts(to)))
	return r.plainList(ctx, where)
}

func (r *GuardShiftRepo) ListByDate(ctx context.Context, date time.Time) ([]model.GuardShift, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	where := fmt.Sprintf("WHERE start_time >= %s AND start_time < %s", sqlStr(ts(dayStart)), sqlStr(ts(dayEnd)))
	return r.plainList(ctx, where)
}

func (r *GuardShiftRepo) plainList(ctx context.Context, where string) ([]model.GuardShift, error) {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := "SELECT " + shiftCols + " FROM guard_shifts " + where + " ORDER BY start_time"
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.GuardShift
	for rows.Next() {
		s, err := scanShift(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return items, rows.Err()
}

func (r *GuardShiftRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.GuardShift], error) {
	tx, err := r.client.BeginDomain(ctx, domainScheduling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM guard_shifts "+where)
	sql := "SELECT " + shiftCols + " FROM guard_shifts " + where + orderClause(f, "start_time") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.GuardShift
	for rows.Next() {
		s, err := scanShift(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return buildListResult(items, total, f), rows.Err()
}

const shiftCols = "id, staff_id, department_id, type, status, start_time, end_time, notes, swapped_with, created_at, updated_at"

func scanShift(rows pgx.Rows) (*model.GuardShift, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	s := &model.GuardShift{}
	if len(vals) >= 11 {
		s.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		s.StaffID = parseUUID(fmt.Sprintf("%v", vals[1]))
		s.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[2]))
		s.Type = model.GuardShiftType(fmt.Sprintf("%v", vals[3]))
		s.Status = model.GuardShiftStatus(fmt.Sprintf("%v", vals[4]))
		s.StartTime = parseTS(fmt.Sprintf("%v", vals[5]))
		s.EndTime = parseTS(fmt.Sprintf("%v", vals[6]))
		s.Notes = fmt.Sprintf("%v", vals[7])
		if v := fmt.Sprintf("%v", vals[8]); v != "" && v != "<nil>" {
			id := parseUUID(v)
			s.SwappedWith = &id
		}
		s.CreatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
		s.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[10]))
	}
	return s, nil
}
