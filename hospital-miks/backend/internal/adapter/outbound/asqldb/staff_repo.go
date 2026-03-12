package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainStaff = "staff"

// ── StaffRepo ───────────────────────────────────────────────────────────────

type StaffRepo struct{ client *Client }

func NewStaffRepo(c *Client) *StaffRepo { return &StaffRepo{client: c} }

func (r *StaffRepo) Create(ctx context.Context, s *model.Staff) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO staff (id, employee_code, first_name, last_name, email, phone, staff_type, specialty, license_number, department_id, hire_date, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(s.ID.String()), sqlStr(s.EmployeeCode),
		sqlStr(s.FirstName), sqlStr(s.LastName),
		sqlStr(s.Email), sqlStr(s.Phone),
		sqlStr(string(s.StaffType)), sqlStr(string(s.Specialty)),
		sqlStr(s.LicenseNumber), sqlStr(s.DepartmentID.String()),
		sqlStr(ts(s.HireDate)), boolToSQL(s.Active),
		sqlStr(ts(s.CreatedAt)), sqlStr(ts(s.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("staff create: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *StaffRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Staff, error) {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT id, employee_code, first_name, last_name, email, phone, staff_type, specialty, license_number, department_id, hire_date, active, created_at, updated_at FROM staff WHERE id = %s", sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("staff not found: %s", id)
	}
	return scanStaff(rows)
}

func (r *StaffRepo) GetByEmployeeCode(ctx context.Context, code string) (*model.Staff, error) {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT id, employee_code, first_name, last_name, email, phone, staff_type, specialty, license_number, department_id, hire_date, active, created_at, updated_at FROM staff WHERE employee_code = %s", sqlStr(code))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("staff not found by code: %s", code)
	}
	return scanStaff(rows)
}

func (r *StaffRepo) Update(ctx context.Context, s *model.Staff) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE staff SET employee_code = %s, first_name = %s, last_name = %s, email = %s, phone = %s, staff_type = %s, specialty = %s, license_number = %s, department_id = %s, hire_date = %s, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(s.EmployeeCode), sqlStr(s.FirstName), sqlStr(s.LastName),
		sqlStr(s.Email), sqlStr(s.Phone),
		sqlStr(string(s.StaffType)), sqlStr(string(s.Specialty)),
		sqlStr(s.LicenseNumber), sqlStr(s.DepartmentID.String()),
		sqlStr(ts(s.HireDate)), boolToSQL(s.Active),
		sqlStr(ts(s.UpdatedAt)), sqlStr(s.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("staff update: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *StaffRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM staff WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *StaffRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Staff], error) {
	return r.listWithWhere(ctx, "", f)
}

func (r *StaffRepo) ListByDepartment(ctx context.Context, deptID uuid.UUID, f model.ListFilter) (*model.ListResult[model.Staff], error) {
	return r.listWithWhere(ctx, fmt.Sprintf("WHERE department_id = %s", sqlStr(deptID.String())), f)
}

func (r *StaffRepo) ListByType(ctx context.Context, staffType model.StaffType, f model.ListFilter) (*model.ListResult[model.Staff], error) {
	return r.listWithWhere(ctx, fmt.Sprintf("WHERE staff_type = %s", sqlStr(string(staffType))), f)
}

func (r *StaffRepo) listWithWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Staff], error) {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	countSQL := "SELECT COUNT(*) FROM staff " + where
	total := countQuery(ctx, tx, countSQL)

	sql := "SELECT id, employee_code, first_name, last_name, email, phone, staff_type, specialty, license_number, department_id, hire_date, active, created_at, updated_at FROM staff " +
		where + orderClause(f, "last_name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.Staff
	for rows.Next() {
		s, err := scanStaff(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *s)
	}
	return buildListResult(items, total, f), rows.Err()
}

// ── DepartmentRepo ──────────────────────────────────────────────────────────

type DepartmentRepo struct{ client *Client }

func NewDepartmentRepo(c *Client) *DepartmentRepo { return &DepartmentRepo{client: c} }

func (r *DepartmentRepo) Create(ctx context.Context, d *model.Department) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO departments (id, name, code, floor, building, head_id, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %d, %s, %s, %s, %s, %s)`,
		sqlStr(d.ID.String()), sqlStr(d.Name), sqlStr(d.Code),
		d.Floor, sqlStr(d.Building), sqlStr(d.HeadID.String()),
		boolToSQL(d.Active),
		sqlStr(ts(d.CreatedAt)), sqlStr(ts(d.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("department create: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *DepartmentRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Department, error) {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT id, name, code, floor, building, head_id, active, created_at, updated_at FROM departments WHERE id = %s", sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("department not found: %s", id)
	}
	return scanDepartment(rows)
}

func (r *DepartmentRepo) Update(ctx context.Context, d *model.Department) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE departments SET name = %s, code = %s, floor = %d, building = %s, head_id = %s, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(d.Name), sqlStr(d.Code), d.Floor, sqlStr(d.Building),
		sqlStr(d.HeadID.String()), boolToSQL(d.Active),
		sqlStr(ts(d.UpdatedAt)), sqlStr(d.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("department update: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *DepartmentRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM departments WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *DepartmentRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Department], error) {
	tx, err := r.client.BeginDomain(ctx, domainStaff)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM departments")

	sql := "SELECT id, name, code, floor, building, head_id, active, created_at, updated_at FROM departments" +
		orderClause(f, "name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.Department
	for rows.Next() {
		d, err := scanDepartment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return buildListResult(items, total, f), rows.Err()
}

// ── Scan helpers ────────────────────────────────────────────────────────────

func scanStaff(rows pgx.Rows) (*model.Staff, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	s := &model.Staff{}
	if len(vals) >= 14 {
		s.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		s.EmployeeCode = fmt.Sprintf("%v", vals[1])
		s.FirstName = fmt.Sprintf("%v", vals[2])
		s.LastName = fmt.Sprintf("%v", vals[3])
		s.Email = fmt.Sprintf("%v", vals[4])
		s.Phone = fmt.Sprintf("%v", vals[5])
		s.StaffType = model.StaffType(fmt.Sprintf("%v", vals[6]))
		s.Specialty = model.Specialty(fmt.Sprintf("%v", vals[7]))
		s.LicenseNumber = fmt.Sprintf("%v", vals[8])
		s.DepartmentID = parseUUID(fmt.Sprintf("%v", vals[9]))
		s.HireDate = parseTS(fmt.Sprintf("%v", vals[10]))
		s.Active = fmt.Sprintf("%v", vals[11]) == "true"
		s.CreatedAt = parseTS(fmt.Sprintf("%v", vals[12]))
		s.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[13]))
	}
	return s, nil
}

func scanDepartment(rows pgx.Rows) (*model.Department, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	d := &model.Department{}
	if len(vals) >= 9 {
		d.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		d.Name = fmt.Sprintf("%v", vals[1])
		d.Code = fmt.Sprintf("%v", vals[2])
		if n, ok := vals[3].(int64); ok {
			d.Floor = int(n)
		}
		d.Building = fmt.Sprintf("%v", vals[4])
		d.HeadID = parseUUID(fmt.Sprintf("%v", vals[5]))
		d.Active = fmt.Sprintf("%v", vals[6]) == "true"
		d.CreatedAt = parseTS(fmt.Sprintf("%v", vals[7]))
		d.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[8]))
	}
	return d, nil
}

// countQuery is a helper to run a COUNT(*) and return the integer result.
func countQuery(ctx context.Context, tx *DomainTx, sql string) int {
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return 0
	}
	defer rows.Close()
	if rows.Next() {
		if vals, err := rows.Values(); err == nil && len(vals) > 0 {
			if n, ok := vals[0].(int64); ok {
				return int(n)
			}
		}
	}
	return 0
}
