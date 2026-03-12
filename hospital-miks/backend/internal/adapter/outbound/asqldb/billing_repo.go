package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainBilling = "billing"

// ── InvoiceRepo ─────────────────────────────────────────────────────────────

type InvoiceRepo struct{ client *Client }

func NewInvoiceRepo(c *Client) *InvoiceRepo { return &InvoiceRepo{client: c} }

func (r *InvoiceRepo) Create(ctx context.Context, inv *model.Invoice) error {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return err
	}
	admID := "NULL"
	if inv.AdmissionID != nil {
		admID = sqlStr(inv.AdmissionID.String())
	}
	sql := fmt.Sprintf(
		`INSERT INTO invoices (id, invoice_number, patient_id, admission_id, status, subtotal, tax, discount, total, currency, issued_at, due_date, paid_at, payment_method, notes, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %f, %f, %f, %f, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(inv.ID.String()), sqlStr(inv.InvoiceNumber),
		sqlStr(inv.PatientID.String()), admID,
		sqlStr(string(inv.Status)),
		inv.Subtotal, inv.Tax, inv.Discount, inv.Total,
		sqlStr(inv.Currency),
		nullableTS(inv.IssuedAt), nullableTS(inv.DueDate), nullableTS(inv.PaidAt),
		sqlStr(string(inv.PaymentMethod)), sqlStr(inv.Notes),
		sqlStr(ts(inv.CreatedAt)), sqlStr(ts(inv.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *InvoiceRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Invoice, error) {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	return r.getOne(ctx, tx, fmt.Sprintf("WHERE id = %s", sqlStr(id.String())))
}

func (r *InvoiceRepo) GetByNumber(ctx context.Context, number string) (*model.Invoice, error) {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	return r.getOne(ctx, tx, fmt.Sprintf("WHERE invoice_number = %s", sqlStr(number)))
}

func (r *InvoiceRepo) Update(ctx context.Context, inv *model.Invoice) error {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return err
	}
	admID := "NULL"
	if inv.AdmissionID != nil {
		admID = sqlStr(inv.AdmissionID.String())
	}
	sql := fmt.Sprintf(
		`UPDATE invoices SET invoice_number = %s, patient_id = %s, admission_id = %s, status = %s, subtotal = %f, tax = %f, discount = %f, total = %f, currency = %s, issued_at = %s, due_date = %s, paid_at = %s, payment_method = %s, notes = %s, updated_at = %s WHERE id = %s`,
		sqlStr(inv.InvoiceNumber), sqlStr(inv.PatientID.String()),
		admID, sqlStr(string(inv.Status)),
		inv.Subtotal, inv.Tax, inv.Discount, inv.Total,
		sqlStr(inv.Currency),
		nullableTS(inv.IssuedAt), nullableTS(inv.DueDate), nullableTS(inv.PaidAt),
		sqlStr(string(inv.PaymentMethod)), sqlStr(inv.Notes),
		sqlStr(ts(inv.UpdatedAt)), sqlStr(inv.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *InvoiceRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Invoice], error) {
	return r.listWhere(ctx, "", f)
}

func (r *InvoiceRepo) ListByPatient(ctx context.Context, pid uuid.UUID, f model.ListFilter) (*model.ListResult[model.Invoice], error) {
	return r.listWhere(ctx, fmt.Sprintf("WHERE patient_id = %s", sqlStr(pid.String())), f)
}

func (r *InvoiceRepo) ListByStatus(ctx context.Context, status model.InvoiceStatus, f model.ListFilter) (*model.ListResult[model.Invoice], error) {
	return r.listWhere(ctx, fmt.Sprintf("WHERE status = %s", sqlStr(string(status))), f)
}

func (r *InvoiceRepo) AddItem(ctx context.Context, item *model.InvoiceItem) error {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO invoice_items (id, invoice_id, description, category, quantity, unit_price, total, created_at)
		 VALUES (%s, %s, %s, %s, %d, %f, %f, %s)`,
		sqlStr(item.ID.String()), sqlStr(item.InvoiceID.String()),
		sqlStr(item.Description), sqlStr(item.Category),
		item.Quantity, item.UnitPrice, item.Total,
		sqlStr(ts(item.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *InvoiceRepo) GetItems(ctx context.Context, invoiceID uuid.UUID) ([]model.InvoiceItem, error) {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, invoice_id, description, category, quantity, unit_price, total, created_at FROM invoice_items WHERE invoice_id = %s ORDER BY created_at",
		sqlStr(invoiceID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.InvoiceItem
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		it := model.InvoiceItem{}
		if len(vals) >= 8 {
			it.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
			it.InvoiceID = parseUUID(fmt.Sprintf("%v", vals[1]))
			it.Description = fmt.Sprintf("%v", vals[2])
			it.Category = fmt.Sprintf("%v", vals[3])
			if n, ok := vals[4].(int64); ok {
				it.Quantity = int(n)
			}
			if f, ok := vals[5].(float64); ok {
				it.UnitPrice = f
			}
			if f, ok := vals[6].(float64); ok {
				it.Total = f
			}
			it.CreatedAt = parseTS(fmt.Sprintf("%v", vals[7]))
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (r *InvoiceRepo) DeleteItem(ctx context.Context, itemID uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM invoice_items WHERE id = %s", sqlStr(itemID.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *InvoiceRepo) getOne(ctx context.Context, tx *DomainTx, where string) (*model.Invoice, error) {
	sql := "SELECT " + invCols + " FROM invoices " + where
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("invoice not found")
	}
	return scanInvoice(rows)
}

func (r *InvoiceRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Invoice], error) {
	tx, err := r.client.BeginDomain(ctx, domainBilling)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM invoices "+where)
	sql := "SELECT " + invCols + " FROM invoices " + where + orderClause(f, "created_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Invoice
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *inv)
	}
	return buildListResult(items, total, f), rows.Err()
}

const invCols = "id, invoice_number, patient_id, admission_id, status, subtotal, tax, discount, total, currency, issued_at, due_date, paid_at, payment_method, notes, created_at, updated_at"

func scanInvoice(rows pgx.Rows) (*model.Invoice, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	inv := &model.Invoice{}
	if len(vals) >= 17 {
		inv.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		inv.InvoiceNumber = fmt.Sprintf("%v", vals[1])
		inv.PatientID = parseUUID(fmt.Sprintf("%v", vals[2]))
		if v := fmt.Sprintf("%v", vals[3]); v != "" && v != "<nil>" {
			id := parseUUID(v)
			inv.AdmissionID = &id
		}
		inv.Status = model.InvoiceStatus(fmt.Sprintf("%v", vals[4]))
		if f, ok := vals[5].(float64); ok {
			inv.Subtotal = f
		}
		if f, ok := vals[6].(float64); ok {
			inv.Tax = f
		}
		if f, ok := vals[7].(float64); ok {
			inv.Discount = f
		}
		if f, ok := vals[8].(float64); ok {
			inv.Total = f
		}
		inv.Currency = fmt.Sprintf("%v", vals[9])
		if v := fmt.Sprintf("%v", vals[10]); v != "" && v != "<nil>" {
			t := parseTS(v)
			inv.IssuedAt = &t
		}
		if v := fmt.Sprintf("%v", vals[11]); v != "" && v != "<nil>" {
			t := parseTS(v)
			inv.DueDate = &t
		}
		if v := fmt.Sprintf("%v", vals[12]); v != "" && v != "<nil>" {
			t := parseTS(v)
			inv.PaidAt = &t
		}
		inv.PaymentMethod = model.PaymentMethod(fmt.Sprintf("%v", vals[13]))
		inv.Notes = fmt.Sprintf("%v", vals[14])
		inv.CreatedAt = parseTS(fmt.Sprintf("%v", vals[15]))
		inv.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[16]))
	}
	return inv, nil
}
