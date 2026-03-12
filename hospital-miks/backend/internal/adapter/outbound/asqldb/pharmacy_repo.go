package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainPharmacy = "pharmacy"

// ── MedicationRepo ──────────────────────────────────────────────────────────

type MedicationRepo struct{ client *Client }

func NewMedicationRepo(c *Client) *MedicationRepo { return &MedicationRepo{client: c} }

func (r *MedicationRepo) Create(ctx context.Context, m *model.Medication) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO medications (id, name, generic_name, code, category, manufacturer, dosage_form, strength, unit, stock_quantity, min_stock, price, requires_prescription, controlled, expiration_date, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %f, %s, %s, %s, %s, %s, %s)`,
		sqlStr(m.ID.String()), sqlStr(m.Name), sqlStr(m.GenericName),
		sqlStr(m.Code), sqlStr(string(m.Category)), sqlStr(m.Manufacturer),
		sqlStr(m.DosageForm), sqlStr(m.Strength), sqlStr(m.Unit),
		m.StockQuantity, m.MinStock, m.Price,
		boolToSQL(m.RequiresRx), boolToSQL(m.Controlled),
		sqlStr(ts(m.ExpirationDate)), boolToSQL(m.Active),
		sqlStr(ts(m.CreatedAt)), sqlStr(ts(m.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MedicationRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Medication, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	return r.getOne(ctx, tx, fmt.Sprintf("WHERE id = %s", sqlStr(id.String())))
}

func (r *MedicationRepo) GetByCode(ctx context.Context, code string) (*model.Medication, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	return r.getOne(ctx, tx, fmt.Sprintf("WHERE code = %s", sqlStr(code)))
}

func (r *MedicationRepo) Update(ctx context.Context, m *model.Medication) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE medications SET name = %s, generic_name = %s, code = %s, category = %s, manufacturer = %s, dosage_form = %s, strength = %s, unit = %s, stock_quantity = %d, min_stock = %d, price = %f, requires_prescription = %s, controlled = %s, expiration_date = %s, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(m.Name), sqlStr(m.GenericName), sqlStr(m.Code),
		sqlStr(string(m.Category)), sqlStr(m.Manufacturer),
		sqlStr(m.DosageForm), sqlStr(m.Strength), sqlStr(m.Unit),
		m.StockQuantity, m.MinStock, m.Price,
		boolToSQL(m.RequiresRx), boolToSQL(m.Controlled),
		sqlStr(ts(m.ExpirationDate)), boolToSQL(m.Active),
		sqlStr(ts(m.UpdatedAt)), sqlStr(m.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MedicationRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM medications WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MedicationRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Medication], error) {
	return r.listWhere(ctx, "", f)
}

func (r *MedicationRepo) ListLowStock(ctx context.Context) ([]model.Medication, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := "SELECT " + medCols + " FROM medications WHERE stock_quantity <= min_stock AND active = true ORDER BY stock_quantity ASC"
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Medication
	for rows.Next() {
		m, err := scanMedication(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

func (r *MedicationRepo) UpdateStock(ctx context.Context, id uuid.UUID, quantity int) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("UPDATE medications SET stock_quantity = %d WHERE id = %s", quantity, sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MedicationRepo) Search(ctx context.Context, query string, f model.ListFilter) (*model.ListResult[model.Medication], error) {
	where := fmt.Sprintf("WHERE name LIKE '%%%s%%' OR generic_name LIKE '%%%s%%' OR code LIKE '%%%s%%'",
		escapeSQL(query), escapeSQL(query), escapeSQL(query))
	return r.listWhere(ctx, where, f)
}

func (r *MedicationRepo) getOne(ctx context.Context, tx *DomainTx, where string) (*model.Medication, error) {
	sql := "SELECT " + medCols + " FROM medications " + where
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("medication not found")
	}
	return scanMedication(rows)
}

func (r *MedicationRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Medication], error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM medications "+where)
	sql := "SELECT " + medCols + " FROM medications " + where + orderClause(f, "name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Medication
	for rows.Next() {
		m, err := scanMedication(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return buildListResult(items, total, f), rows.Err()
}

const medCols = "id, name, generic_name, code, category, manufacturer, dosage_form, strength, unit, stock_quantity, min_stock, price, requires_prescription, controlled, expiration_date, active, created_at, updated_at"

func scanMedication(rows pgx.Rows) (*model.Medication, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	m := &model.Medication{}
	if len(vals) >= 18 {
		m.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		m.Name = fmt.Sprintf("%v", vals[1])
		m.GenericName = fmt.Sprintf("%v", vals[2])
		m.Code = fmt.Sprintf("%v", vals[3])
		m.Category = model.MedicationCategory(fmt.Sprintf("%v", vals[4]))
		m.Manufacturer = fmt.Sprintf("%v", vals[5])
		m.DosageForm = fmt.Sprintf("%v", vals[6])
		m.Strength = fmt.Sprintf("%v", vals[7])
		m.Unit = fmt.Sprintf("%v", vals[8])
		if n, ok := vals[9].(int64); ok {
			m.StockQuantity = int(n)
		}
		if n, ok := vals[10].(int64); ok {
			m.MinStock = int(n)
		}
		if f, ok := vals[11].(float64); ok {
			m.Price = f
		}
		m.RequiresRx = fmt.Sprintf("%v", vals[12]) == "true"
		m.Controlled = fmt.Sprintf("%v", vals[13]) == "true"
		m.ExpirationDate = parseTS(fmt.Sprintf("%v", vals[14]))
		m.Active = fmt.Sprintf("%v", vals[15]) == "true"
		m.CreatedAt = parseTS(fmt.Sprintf("%v", vals[16]))
		m.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[17]))
	}
	return m, nil
}

// ── PrescriptionRepo ────────────────────────────────────────────────────────

type PrescriptionRepo struct{ client *Client }

func NewPrescriptionRepo(c *Client) *PrescriptionRepo { return &PrescriptionRepo{client: c} }

func (r *PrescriptionRepo) Create(ctx context.Context, rx *model.Prescription) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO prescriptions (id, patient_id, doctor_id, medication_id, status, dosage, frequency, duration, instructions, quantity, refills_allowed, refills_used, prescribed_at, dispensed_at, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %d, %s, %s, %s, %s)`,
		sqlStr(rx.ID.String()), sqlStr(rx.PatientID.String()),
		sqlStr(rx.DoctorID.String()), sqlStr(rx.MedicationID.String()),
		sqlStr(string(rx.Status)), sqlStr(rx.Dosage),
		sqlStr(rx.Frequency), sqlStr(rx.Duration), sqlStr(rx.Instructions),
		rx.Quantity, rx.Refills, rx.RefillsUsed,
		sqlStr(ts(rx.PrescribedAt)), nullableTS(rx.DispensedAt),
		sqlStr(ts(rx.CreatedAt)), sqlStr(ts(rx.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *PrescriptionRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Prescription, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM prescriptions WHERE id = %s", rxCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("prescription not found: %s", id)
	}
	return scanPrescription(rows)
}

func (r *PrescriptionRepo) Update(ctx context.Context, rx *model.Prescription) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE prescriptions SET patient_id = %s, doctor_id = %s, medication_id = %s, status = %s, dosage = %s, frequency = %s, duration = %s, instructions = %s, quantity = %d, refills_allowed = %d, refills_used = %d, prescribed_at = %s, dispensed_at = %s, updated_at = %s WHERE id = %s`,
		sqlStr(rx.PatientID.String()), sqlStr(rx.DoctorID.String()),
		sqlStr(rx.MedicationID.String()), sqlStr(string(rx.Status)),
		sqlStr(rx.Dosage), sqlStr(rx.Frequency),
		sqlStr(rx.Duration), sqlStr(rx.Instructions),
		rx.Quantity, rx.Refills, rx.RefillsUsed,
		sqlStr(ts(rx.PrescribedAt)), nullableTS(rx.DispensedAt),
		sqlStr(ts(rx.UpdatedAt)), sqlStr(rx.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *PrescriptionRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Prescription], error) {
	return r.listWhere(ctx, "", f)
}

func (r *PrescriptionRepo) ListByPatient(ctx context.Context, pid uuid.UUID) ([]model.Prescription, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM prescriptions WHERE patient_id = %s ORDER BY prescribed_at DESC", rxCols, sqlStr(pid.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Prescription
	for rows.Next() {
		rx, err := scanPrescription(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *rx)
	}
	return items, rows.Err()
}

func (r *PrescriptionRepo) ListActive(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Prescription], error) {
	return r.listWhere(ctx, "WHERE status = 'ACTIVE'", f)
}

func (r *PrescriptionRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Prescription], error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM prescriptions "+where)
	sql := "SELECT " + rxCols + " FROM prescriptions " + where + orderClause(f, "prescribed_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Prescription
	for rows.Next() {
		rx, err := scanPrescription(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *rx)
	}
	return buildListResult(items, total, f), rows.Err()
}

const rxCols = "id, patient_id, doctor_id, medication_id, status, dosage, frequency, duration, instructions, quantity, refills_allowed, refills_used, prescribed_at, dispensed_at, created_at, updated_at"

func scanPrescription(rows pgx.Rows) (*model.Prescription, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	rx := &model.Prescription{}
	if len(vals) >= 16 {
		rx.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		rx.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		rx.DoctorID = parseUUID(fmt.Sprintf("%v", vals[2]))
		rx.MedicationID = parseUUID(fmt.Sprintf("%v", vals[3]))
		rx.Status = model.PrescriptionStatus(fmt.Sprintf("%v", vals[4]))
		rx.Dosage = fmt.Sprintf("%v", vals[5])
		rx.Frequency = fmt.Sprintf("%v", vals[6])
		rx.Duration = fmt.Sprintf("%v", vals[7])
		rx.Instructions = fmt.Sprintf("%v", vals[8])
		if n, ok := vals[9].(int64); ok {
			rx.Quantity = int(n)
		}
		if n, ok := vals[10].(int64); ok {
			rx.Refills = int(n)
		}
		if n, ok := vals[11].(int64); ok {
			rx.RefillsUsed = int(n)
		}
		rx.PrescribedAt = parseTS(fmt.Sprintf("%v", vals[12]))
		if v := fmt.Sprintf("%v", vals[13]); v != "" && v != "<nil>" {
			t := parseTS(v)
			rx.DispensedAt = &t
		}
		rx.CreatedAt = parseTS(fmt.Sprintf("%v", vals[14]))
		rx.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[15]))
	}
	return rx, nil
}

// ── PharmacyDispenseRepo ────────────────────────────────────────────────────

type PharmacyDispenseRepo struct{ client *Client }

func NewPharmacyDispenseRepo(c *Client) *PharmacyDispenseRepo {
	return &PharmacyDispenseRepo{client: c}
}

func (r *PharmacyDispenseRepo) Create(ctx context.Context, d *model.PharmacyDispense) error {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO pharmacy_dispenses (id, prescription_id, pharmacist_id, quantity, notes, dispensed_at, created_at)
		 VALUES (%s, %s, %s, %d, %s, %s, %s)`,
		sqlStr(d.ID.String()), sqlStr(d.PrescriptionID.String()),
		sqlStr(d.PharmacistID.String()), d.Quantity,
		sqlStr(d.Notes), sqlStr(ts(d.DispensedAt)), sqlStr(ts(d.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *PharmacyDispenseRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.PharmacyDispense, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, prescription_id, pharmacist_id, quantity, notes, dispensed_at, created_at FROM pharmacy_dispenses WHERE id = %s", sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("dispense not found: %s", id)
	}
	return scanDispense(rows)
}

func (r *PharmacyDispenseRepo) ListByPrescription(ctx context.Context, rxID uuid.UUID) ([]model.PharmacyDispense, error) {
	tx, err := r.client.BeginDomain(ctx, domainPharmacy)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT id, prescription_id, pharmacist_id, quantity, notes, dispensed_at, created_at FROM pharmacy_dispenses WHERE prescription_id = %s ORDER BY dispensed_at DESC",
		sqlStr(rxID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.PharmacyDispense
	for rows.Next() {
		d, err := scanDispense(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return items, rows.Err()
}

func scanDispense(rows pgx.Rows) (*model.PharmacyDispense, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	d := &model.PharmacyDispense{}
	if len(vals) >= 7 {
		d.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		d.PrescriptionID = parseUUID(fmt.Sprintf("%v", vals[1]))
		d.PharmacistID = parseUUID(fmt.Sprintf("%v", vals[2]))
		if n, ok := vals[3].(int64); ok {
			d.Quantity = int(n)
		}
		d.Notes = fmt.Sprintf("%v", vals[4])
		d.DispensedAt = parseTS(fmt.Sprintf("%v", vals[5]))
		d.CreatedAt = parseTS(fmt.Sprintf("%v", vals[6]))
	}
	return d, nil
}
