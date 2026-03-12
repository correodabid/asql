package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainDocuments = "documents"

// ── DocumentRepo ────────────────────────────────────────────────────────────

type DocumentRepo struct{ client *Client }

func NewDocumentRepo(c *Client) *DocumentRepo { return &DocumentRepo{client: c} }

func (r *DocumentRepo) Create(ctx context.Context, d *model.Document) error {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return err
	}
	patID := "NULL"
	if d.PatientID != nil {
		patID = sqlStr(d.PatientID.String())
	}
	sql := fmt.Sprintf(
		`INSERT INTO documents (id, title, category, patient_id, uploaded_by, file_name, mime_type, size_bytes, storage_path, checksum, version, tags, notes, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %d, %s, %s, %s, %s)`,
		sqlStr(d.ID.String()), sqlStr(d.Title), sqlStr(string(d.Category)),
		patID, sqlStr(d.UploadedBy.String()),
		sqlStr(d.FileName), sqlStr(d.MimeType), d.SizeBytes,
		sqlStr(d.StoragePath), sqlStr(d.Checksum), d.Version,
		sqlStr(d.Tags), sqlStr(d.Notes),
		sqlStr(ts(d.CreatedAt)), sqlStr(ts(d.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *DocumentRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Document, error) {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM documents WHERE id = %s", docCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("document not found: %s", id)
	}
	return scanDoc(rows)
}

func (r *DocumentRepo) Update(ctx context.Context, d *model.Document) error {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return err
	}
	patID := "NULL"
	if d.PatientID != nil {
		patID = sqlStr(d.PatientID.String())
	}
	sql := fmt.Sprintf(
		`UPDATE documents SET title = %s, category = %s, patient_id = %s, uploaded_by = %s, file_name = %s, mime_type = %s, size_bytes = %d, storage_path = %s, checksum = %s, version = %d, tags = %s, notes = %s, updated_at = %s WHERE id = %s`,
		sqlStr(d.Title), sqlStr(string(d.Category)),
		patID, sqlStr(d.UploadedBy.String()),
		sqlStr(d.FileName), sqlStr(d.MimeType), d.SizeBytes,
		sqlStr(d.StoragePath), sqlStr(d.Checksum), d.Version,
		sqlStr(d.Tags), sqlStr(d.Notes),
		sqlStr(ts(d.UpdatedAt)), sqlStr(d.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *DocumentRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM documents WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *DocumentRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Document], error) {
	return r.listWhere(ctx, "", f)
}

func (r *DocumentRepo) ListByPatient(ctx context.Context, pid uuid.UUID, f model.ListFilter) (*model.ListResult[model.Document], error) {
	where := fmt.Sprintf("WHERE patient_id = %s", sqlStr(pid.String()))
	return r.listWhere(ctx, where, f)
}

func (r *DocumentRepo) ListByCategory(ctx context.Context, cat model.DocumentCategory, f model.ListFilter) (*model.ListResult[model.Document], error) {
	where := fmt.Sprintf("WHERE category = %s", sqlStr(string(cat)))
	return r.listWhere(ctx, where, f)
}

func (r *DocumentRepo) Search(ctx context.Context, query string, f model.ListFilter) (*model.ListResult[model.Document], error) {
	where := fmt.Sprintf("WHERE title LIKE '%%%s%%' OR tags LIKE '%%%s%%' OR notes LIKE '%%%s%%'",
		escapeSQL(query), escapeSQL(query), escapeSQL(query))
	return r.listWhere(ctx, where, f)
}

func (r *DocumentRepo) LogAccess(ctx context.Context, a *model.DocumentAccess) error {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO document_access_log (id, document_id, staff_id, action, ip_address, accessed_at)
		 VALUES (%s, %s, %s, %s, %s, %s)`,
		sqlStr(a.ID.String()), sqlStr(a.DocumentID.String()),
		sqlStr(a.StaffID.String()), sqlStr(a.Action),
		sqlStr(a.IPAddress), sqlStr(ts(a.AccessedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *DocumentRepo) GetAccessLog(ctx context.Context, docID uuid.UUID) ([]model.DocumentAccess, error) {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, document_id, staff_id, action, ip_address, accessed_at FROM document_access_log WHERE document_id = %s ORDER BY accessed_at DESC",
		sqlStr(docID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.DocumentAccess
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		a := model.DocumentAccess{}
		if len(vals) >= 6 {
			a.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
			a.DocumentID = parseUUID(fmt.Sprintf("%v", vals[1]))
			a.StaffID = parseUUID(fmt.Sprintf("%v", vals[2]))
			a.Action = fmt.Sprintf("%v", vals[3])
			a.IPAddress = fmt.Sprintf("%v", vals[4])
			a.AccessedAt = parseTS(fmt.Sprintf("%v", vals[5]))
		}
		items = append(items, a)
	}
	return items, rows.Err()
}

func (r *DocumentRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Document], error) {
	tx, err := r.client.BeginDomain(ctx, domainDocuments)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM documents "+where)
	sql := "SELECT " + docCols + " FROM documents " + where + orderClause(f, "created_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Document
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return buildListResult(items, total, f), rows.Err()
}

const docCols = "id, title, category, patient_id, uploaded_by, file_name, mime_type, size_bytes, storage_path, checksum, version, tags, notes, created_at, updated_at"

func scanDoc(rows pgx.Rows) (*model.Document, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	d := &model.Document{}
	if len(vals) >= 15 {
		d.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		d.Title = fmt.Sprintf("%v", vals[1])
		d.Category = model.DocumentCategory(fmt.Sprintf("%v", vals[2]))
		if v := fmt.Sprintf("%v", vals[3]); v != "" && v != "<nil>" {
			id := parseUUID(v)
			d.PatientID = &id
		}
		d.UploadedBy = parseUUID(fmt.Sprintf("%v", vals[4]))
		d.FileName = fmt.Sprintf("%v", vals[5])
		d.MimeType = fmt.Sprintf("%v", vals[6])
		if n, ok := vals[7].(int64); ok {
			d.SizeBytes = n
		}
		d.StoragePath = fmt.Sprintf("%v", vals[8])
		d.Checksum = fmt.Sprintf("%v", vals[9])
		if n, ok := vals[10].(int64); ok {
			d.Version = int(n)
		}
		d.Tags = fmt.Sprintf("%v", vals[11])
		d.Notes = fmt.Sprintf("%v", vals[12])
		d.CreatedAt = parseTS(fmt.Sprintf("%v", vals[13]))
		d.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[14]))
	}
	return d, nil
}
