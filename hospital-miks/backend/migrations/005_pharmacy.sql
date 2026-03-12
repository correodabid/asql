-- Hospital MiKS – Domain: pharmacy
-- Medications, prescriptions, and dispenses.

BEGIN DOMAIN pharmacy;

CREATE TABLE medications (
    id               TEXT PRIMARY KEY DEFAULT UUID_V7,
    name             TEXT NOT NULL,
    generic_name     TEXT NOT NULL DEFAULT '',
    code             TEXT NOT NULL UNIQUE,
    category         TEXT NOT NULL,
    manufacturer     TEXT NOT NULL DEFAULT '',
    dosage_form      TEXT NOT NULL DEFAULT '',
    strength         TEXT NOT NULL DEFAULT '',
    unit             TEXT NOT NULL DEFAULT '',
    stock_quantity   INT NOT NULL DEFAULT 0,
    min_stock        INT NOT NULL DEFAULT 0,
    price            FLOAT NOT NULL DEFAULT 0,
    requires_rx      BOOL NOT NULL DEFAULT true,
    controlled       BOOL NOT NULL DEFAULT false,
    expiration_date  TIMESTAMP,
    active           BOOL NOT NULL DEFAULT true,
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL
);

CREATE INDEX idx_medications_category ON medications(category);

CREATE TABLE prescriptions (
    id             TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id     TEXT NOT NULL,
    doctor_id      TEXT NOT NULL,
    medication_id  TEXT NOT NULL REFERENCES medications(id),
    status         TEXT NOT NULL DEFAULT 'ACTIVE',
    dosage         TEXT NOT NULL,
    frequency      TEXT NOT NULL,
    duration       TEXT NOT NULL DEFAULT '',
    instructions   TEXT NOT NULL DEFAULT '',
    quantity       INT NOT NULL DEFAULT 1,
    refills        INT NOT NULL DEFAULT 0,
    refills_used   INT NOT NULL DEFAULT 0,
    prescribed_at  TIMESTAMP NOT NULL,
    dispensed_at   TIMESTAMP,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL,

    -- VFKs: patient and prescribing doctor
    patient_lsn INT,
    doctor_lsn INT,
    VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (doctor_id)  REFERENCES staff.staff(id)       AS OF doctor_lsn
);

CREATE INDEX idx_prescriptions_patient ON prescriptions(patient_id);
CREATE INDEX idx_prescriptions_status ON prescriptions(status);

CREATE TABLE pharmacy_dispenses (
    id              TEXT PRIMARY KEY DEFAULT UUID_V7,
    prescription_id TEXT NOT NULL REFERENCES prescriptions(id),
    pharmacist_id   TEXT NOT NULL,
    quantity        INT NOT NULL,
    notes           TEXT NOT NULL DEFAULT '',
    dispensed_at    TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL,

    pharmacist_lsn INT,
    VERSIONED FOREIGN KEY (pharmacist_id) REFERENCES staff.staff(id) AS OF pharmacist_lsn
);

-- Entity: Prescription is the aggregate root
CREATE ENTITY prescription_entity (ROOT prescriptions, INCLUDES pharmacy_dispenses);

COMMIT;
