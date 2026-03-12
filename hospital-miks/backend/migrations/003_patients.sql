-- Hospital MiKS – Domain: patients
-- Patient registry – the richest audit domain.

BEGIN DOMAIN patients;

CREATE TABLE patients (
    id                      TEXT PRIMARY KEY DEFAULT UUID_V7,
    medical_record_no       TEXT NOT NULL UNIQUE,
    first_name              TEXT NOT NULL,
    last_name               TEXT NOT NULL,
    date_of_birth           TIMESTAMP NOT NULL,
    gender                  TEXT NOT NULL,
    national_id             TEXT NOT NULL UNIQUE,
    phone                   TEXT NOT NULL DEFAULT '',
    email                   TEXT NOT NULL DEFAULT '',
    address                 TEXT NOT NULL DEFAULT '',
    city                    TEXT NOT NULL DEFAULT '',
    postal_code             TEXT NOT NULL DEFAULT '',
    blood_type              TEXT NOT NULL DEFAULT '',
    allergies               TEXT NOT NULL DEFAULT '',
    emergency_contact_name  TEXT NOT NULL DEFAULT '',
    emergency_contact_phone TEXT NOT NULL DEFAULT '',
    insurance_id            TEXT NOT NULL DEFAULT '',
    insurance_company       TEXT NOT NULL DEFAULT '',
    active                  BOOL NOT NULL DEFAULT true,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
);


-- Entity: Patient is the aggregate root (changes auto-increment entity version)
CREATE ENTITY patient_entity (ROOT patients);

COMMIT;
