package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainPatients = "patients"

// PatientRepo implements port.PatientRepository against the ASQL "patients" domain.
type PatientRepo struct{ client *Client }

func NewPatientRepo(c *Client) *PatientRepo { return &PatientRepo{client: c} }

func (r *PatientRepo) Create(ctx context.Context, p *model.Patient) error {
	tx, err := r.client.BeginDomain(ctx, domainPatients)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO patients (id, medical_record_no, first_name, last_name, date_of_birth, gender, national_id, phone, email, address, city, postal_code, blood_type, allergies, emergency_contact_name, emergency_contact_phone, insurance_id, insurance_company, active, created_at, updated_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(p.ID.String()), sqlStr(p.MedicalRecordNo),
		sqlStr(p.FirstName), sqlStr(p.LastName),
		sqlStr(ts(p.DateOfBirth)), sqlStr(string(p.Gender)),
		sqlStr(p.NationalID), sqlStr(p.Phone), sqlStr(p.Email),
		sqlStr(p.Address), sqlStr(p.City), sqlStr(p.PostalCode),
		sqlStr(string(p.BloodType)), sqlStr(p.Allergies),
		sqlStr(p.EmergencyName), sqlStr(p.EmergencyPhone),
		sqlStr(p.InsuranceID), sqlStr(p.InsuranceCompany),
		boolToSQL(p.Active),
		sqlStr(ts(p.CreatedAt)), sqlStr(ts(p.UpdatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("patient create: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *PatientRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Patient, error) {
	return r.getByField(ctx, "id", id.String())
}

func (r *PatientRepo) GetByMedicalRecordNo(ctx context.Context, mrn string) (*model.Patient, error) {
	return r.getByField(ctx, "medical_record_no", mrn)
}

func (r *PatientRepo) GetByNationalID(ctx context.Context, nid string) (*model.Patient, error) {
	return r.getByField(ctx, "national_id", nid)
}

func (r *PatientRepo) getByField(ctx context.Context, field, value string) (*model.Patient, error) {
	tx, err := r.client.BeginDomain(ctx, domainPatients)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT %s FROM patients WHERE %s = %s", patientCols, field, sqlStr(value))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("patient not found by %s: %s", field, value)
	}
	return scanPatient(rows)
}

func (r *PatientRepo) Update(ctx context.Context, p *model.Patient) error {
	tx, err := r.client.BeginDomain(ctx, domainPatients)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`UPDATE patients SET medical_record_no = %s, first_name = %s, last_name = %s, date_of_birth = %s, gender = %s, national_id = %s, phone = %s, email = %s, address = %s, city = %s, postal_code = %s, blood_type = %s, allergies = %s, emergency_contact_name = %s, emergency_contact_phone = %s, insurance_id = %s, insurance_company = %s, active = %s, updated_at = %s WHERE id = %s`,
		sqlStr(p.MedicalRecordNo), sqlStr(p.FirstName), sqlStr(p.LastName),
		sqlStr(ts(p.DateOfBirth)), sqlStr(string(p.Gender)),
		sqlStr(p.NationalID), sqlStr(p.Phone), sqlStr(p.Email),
		sqlStr(p.Address), sqlStr(p.City), sqlStr(p.PostalCode),
		sqlStr(string(p.BloodType)), sqlStr(p.Allergies),
		sqlStr(p.EmergencyName), sqlStr(p.EmergencyPhone),
		sqlStr(p.InsuranceID), sqlStr(p.InsuranceCompany),
		boolToSQL(p.Active),
		sqlStr(ts(p.UpdatedAt)), sqlStr(p.ID.String()),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return fmt.Errorf("patient update: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *PatientRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainPatients)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM patients WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *PatientRepo) List(ctx context.Context, f model.ListFilter) (*model.ListResult[model.Patient], error) {
	return r.listWithWhere(ctx, "", f)
}

func (r *PatientRepo) Search(ctx context.Context, query string, f model.ListFilter) (*model.ListResult[model.Patient], error) {
	where := fmt.Sprintf("WHERE first_name LIKE %s OR last_name LIKE %s OR national_id LIKE %s OR medical_record_no LIKE %s",
		sqlStr("%"+query+"%"), sqlStr("%"+query+"%"), sqlStr("%"+query+"%"), sqlStr("%"+query+"%"))
	return r.listWithWhere(ctx, where, f)
}

func (r *PatientRepo) listWithWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Patient], error) {
	tx, err := r.client.BeginDomain(ctx, domainPatients)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM patients "+where)

	sql := "SELECT " + patientCols + " FROM patients " + where +
		orderClause(f, "last_name") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.Patient
	for rows.Next() {
		p, err := scanPatient(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *p)
	}
	return buildListResult(items, total, f), rows.Err()
}

const patientCols = "id, medical_record_no, first_name, last_name, date_of_birth, gender, national_id, phone, email, address, city, postal_code, blood_type, allergies, emergency_contact_name, emergency_contact_phone, insurance_id, insurance_company, active, created_at, updated_at"

func scanPatient(rows pgx.Rows) (*model.Patient, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	p := &model.Patient{}
	if len(vals) >= 21 {
		p.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		p.MedicalRecordNo = fmt.Sprintf("%v", vals[1])
		p.FirstName = fmt.Sprintf("%v", vals[2])
		p.LastName = fmt.Sprintf("%v", vals[3])
		p.DateOfBirth = parseTS(fmt.Sprintf("%v", vals[4]))
		p.Gender = model.Gender(fmt.Sprintf("%v", vals[5]))
		p.NationalID = fmt.Sprintf("%v", vals[6])
		p.Phone = fmt.Sprintf("%v", vals[7])
		p.Email = fmt.Sprintf("%v", vals[8])
		p.Address = fmt.Sprintf("%v", vals[9])
		p.City = fmt.Sprintf("%v", vals[10])
		p.PostalCode = fmt.Sprintf("%v", vals[11])
		p.BloodType = model.BloodType(fmt.Sprintf("%v", vals[12]))
		p.Allergies = fmt.Sprintf("%v", vals[13])
		p.EmergencyName = fmt.Sprintf("%v", vals[14])
		p.EmergencyPhone = fmt.Sprintf("%v", vals[15])
		p.InsuranceID = fmt.Sprintf("%v", vals[16])
		p.InsuranceCompany = fmt.Sprintf("%v", vals[17])
		p.Active = fmt.Sprintf("%v", vals[18]) == "true"
		p.CreatedAt = parseTS(fmt.Sprintf("%v", vals[19]))
		p.UpdatedAt = parseTS(fmt.Sprintf("%v", vals[20]))
	}
	return p, nil
}
