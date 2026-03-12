-- Hospital MiKS – Domain: rehab
-- Rehabilitation plans and sessions.

BEGIN DOMAIN rehab;

CREATE TABLE rehab_plans (
    id                 TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id         TEXT NOT NULL,
    therapist_id       TEXT NOT NULL,
    doctor_id          TEXT NOT NULL,
    type               TEXT NOT NULL,
    diagnosis          TEXT NOT NULL DEFAULT '',
    goals              TEXT NOT NULL DEFAULT '',
    start_date         TIMESTAMP NOT NULL,
    end_date           TIMESTAMP,
    sessions           INT NOT NULL DEFAULT 0,
    completed          INT NOT NULL DEFAULT 0,
    active             BOOL NOT NULL DEFAULT true,
    notes              TEXT NOT NULL DEFAULT '',
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL,

    -- VFKs
    patient_lsn INT,
    therapist_lsn INT,
    doctor_lsn INT,
    VERSIONED FOREIGN KEY (patient_id)   REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (therapist_id) REFERENCES staff.staff(id)       AS OF therapist_lsn,
    VERSIONED FOREIGN KEY (doctor_id)    REFERENCES staff.staff(id)       AS OF doctor_lsn
);

CREATE INDEX idx_rehab_plans_patient ON rehab_plans(patient_id);

CREATE TABLE rehab_sessions (
    id            TEXT PRIMARY KEY DEFAULT UUID_V7,
    plan_id       TEXT NOT NULL REFERENCES rehab_plans(id),
    therapist_id  TEXT NOT NULL,
    patient_id    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'SCHEDULED',
    scheduled_at  TIMESTAMP NOT NULL,
    duration      INT NOT NULL DEFAULT 45,
    room          TEXT NOT NULL DEFAULT '',
    exercises     TEXT NOT NULL DEFAULT '',
    progress      TEXT NOT NULL DEFAULT '',
    pain_level    INT,
    notes         TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,

    therapist_lsn INT,
    patient_lsn INT,
    VERSIONED FOREIGN KEY (therapist_id) REFERENCES staff.staff(id)       AS OF therapist_lsn,
    VERSIONED FOREIGN KEY (patient_id)   REFERENCES patients.patients(id) AS OF patient_lsn
);

-- Entity: RehabPlan aggregate (root + sessions)
CREATE ENTITY rehab_entity (ROOT rehab_plans, INCLUDES rehab_sessions);

COMMIT;
