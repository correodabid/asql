package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// ── UserRepo ────────────────────────────────────────────────────────────────

const domainIdentity = "identity"

// UserRepo implements port.UserRepository against the ASQL "identity" domain.
type UserRepo struct{ client *Client }

func NewUserRepo(c *Client) *UserRepo { return &UserRepo{client: c} }

func (r *UserRepo) Create(ctx context.Context, u *model.User) error {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO users (id, staff_id, username, password_hash, role, active, last_login_at, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(u.ID.String()), sqlStr(u.StaffID.String()),
		sqlStr(u.Username), sqlStr(u.PasswordHash),
		sqlStr(u.Role), boolToSQL(u.Active),
		nullableTS(u.LastLoginAt),
		sqlStr(ts(u.CreatedAt)), sqlStr(ts(u.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("user create: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *UserRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.User, error) {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT id, staff_id, username, password_hash, role, active, last_login_at, created_at, updated_at FROM users WHERE id = %s", sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("user not found: %s", id)
	}
	return scanUser(rows)
}

func (r *UserRepo) GetByUsername(ctx context.Context, username string) (*model.User, error) {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT id, staff_id, username, password_hash, role, active, last_login_at, created_at, updated_at FROM users WHERE username = %s", sqlStr(username))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("user not found: %s", username)
	}
	return scanUser(rows)
}

func (r *UserRepo) Update(ctx context.Context, u *model.User) error {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE users SET staff_id = %s, username = %s, password_hash = %s, role = %s, active = %s, last_login_at = %s, updated_at = %s WHERE id = %s`,
		sqlStr(u.StaffID.String()), sqlStr(u.Username), sqlStr(u.PasswordHash),
		sqlStr(u.Role), boolToSQL(u.Active),
		nullableTS(u.LastLoginAt),
		sqlStr(ts(u.UpdatedAt)), sqlStr(u.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("user update: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *UserRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM users WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *UserRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.User], error) {
	tx, err := r.client.BeginDomain(ctx, domainIdentity)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Count
	countRows, err := tx.Query(ctx, "SELECT COUNT(*) FROM users")
	if err != nil {
		return nil, err
	}
	var total int
	if countRows.Next() {
		if vals, err := countRows.Values(); err == nil && len(vals) > 0 {
			if n, ok := vals[0].(int64); ok {
				total = int(n)
			}
		}
	}
	countRows.Close()

	sql := "SELECT id, staff_id, username, password_hash, role, active, last_login_at, created_at, updated_at FROM users" +
		orderClause(f, "created_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *u)
	}
	return buildListResult(items, total, f), rows.Err()
}

// scanUser scans a single user row from pgx.Rows using Values().
func scanUser(rows interface{ Values() ([]any, error) }) (*model.User, error) {
	vals, err := rows.(interface{ Values() ([]any, error) }).Values()
	if err != nil {
		return nil, err
	}
	u := &model.User{}
	if len(vals) >= 9 {
		u.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		u.StaffID = parseUUID(fmt.Sprintf("%v", vals[1]))
		u.Username = fmt.Sprintf("%v", vals[2])
		u.PasswordHash = fmt.Sprintf("%v", vals[3])
		u.Role = fmt.Sprintf("%v", vals[4])
		u.Active = fmt.Sprintf("%v", vals[5]) == "true"
		if s := fmt.Sprintf("%v", vals[6]); s != "" && s != "<nil>" {
			t := parseTS(s)
			u.LastLoginAt = &t
		}
		u.CreatedAt = parseTS(fmt.Sprintf("%v", vals[7]))
		u.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[8]))
	}
	return u, nil
}
