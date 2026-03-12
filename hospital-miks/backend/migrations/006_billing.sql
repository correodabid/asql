-- Hospital MiKS – Domain: billing
-- Invoices and line items.

BEGIN DOMAIN billing;

CREATE TABLE invoices (
    id              TEXT PRIMARY KEY DEFAULT UUID_V7,
    invoice_number  TEXT NOT NULL UNIQUE,
    patient_id      TEXT NOT NULL,
    admission_id    TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'DRAFT',
    subtotal        FLOAT NOT NULL DEFAULT 0,
    tax             FLOAT NOT NULL DEFAULT 0,
    discount        FLOAT NOT NULL DEFAULT 0,
    total           FLOAT NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'EUR',
    issued_at       TIMESTAMP,
    due_date        TIMESTAMP,
    paid_at         TIMESTAMP,
    payment_method  TEXT NOT NULL DEFAULT '',
    notes           TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,

    -- VFKs
    patient_lsn INT,
    admission_lsn INT,
    VERSIONED FOREIGN KEY (patient_id)   REFERENCES patients.patients(id)      AS OF patient_lsn,
    VERSIONED FOREIGN KEY (admission_id) REFERENCES clinical.admissions(id)    AS OF admission_lsn
);

CREATE INDEX idx_invoices_patient ON invoices(patient_id);
CREATE INDEX idx_invoices_status ON invoices(status);

CREATE TABLE invoice_items (
    id          TEXT PRIMARY KEY DEFAULT UUID_V7,
    invoice_id  TEXT NOT NULL REFERENCES invoices(id),
    description TEXT NOT NULL,
    category    TEXT NOT NULL DEFAULT '',
    quantity    INT NOT NULL DEFAULT 1,
    unit_price  FLOAT NOT NULL DEFAULT 0,
    total       FLOAT NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL
);

-- Entity: Invoice aggregate (root + items)
CREATE ENTITY invoice_entity (ROOT invoices, INCLUDES invoice_items);

COMMIT;
