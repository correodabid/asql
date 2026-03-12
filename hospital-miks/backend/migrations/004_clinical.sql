-- Hospital MiKS – Domain: clinical
-- Appointments, surgeries, admissions, wards, beds, rooms.
-- Heavy use of VFKs pointing to staff and patients domains.

BEGIN DOMAIN clinical;

-- ── Consultation Rooms ─────────────────────────────────────
CREATE TABLE consultation_rooms (
    id             TEXT PRIMARY KEY DEFAULT UUID_V7,
    name           TEXT NOT NULL,
    code           TEXT NOT NULL UNIQUE,
    department_id  TEXT NOT NULL,
    floor          INT NOT NULL DEFAULT 0,
    building       TEXT NOT NULL DEFAULT '',
    equipment      TEXT NOT NULL DEFAULT '',
    active         BOOL NOT NULL DEFAULT true,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL,

    -- VFK: department lives in staff domain, capture version at insert.
    department_lsn INT,
    VERSIONED FOREIGN KEY (department_id) REFERENCES staff.departments(id) AS OF department_lsn
);

-- ── Appointments ───────────────────────────────────────────
CREATE TABLE appointments (
    id               TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id       TEXT NOT NULL,
    doctor_id        TEXT NOT NULL,
    department_id    TEXT NOT NULL DEFAULT '',
    type             TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'SCHEDULED',
    scheduled_at     TIMESTAMP NOT NULL,
    duration_minutes INT NOT NULL DEFAULT 30,
    room             TEXT NOT NULL DEFAULT '',
    notes            TEXT NOT NULL DEFAULT '',
    diagnosis        TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL,

    -- VFKs: cross-domain references
    patient_lsn INT,
    doctor_lsn INT,
    VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (doctor_id)  REFERENCES staff.staff(id)       AS OF doctor_lsn
);

CREATE INDEX idx_appointments_patient ON appointments(patient_id);
CREATE INDEX idx_appointments_doctor ON appointments(doctor_id);
CREATE INDEX idx_appointments_scheduled ON appointments(scheduled_at);

-- ── Operating Rooms ────────────────────────────────────────
CREATE TABLE operating_rooms (
    id         TEXT PRIMARY KEY DEFAULT UUID_V7,
    name       TEXT NOT NULL,
    code       TEXT NOT NULL UNIQUE,
    floor      INT NOT NULL DEFAULT 0,
    building   TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'AVAILABLE',
    equipment  TEXT NOT NULL DEFAULT '',
    capacity   INT NOT NULL DEFAULT 1,
    active     BOOL NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- ── Surgeries ──────────────────────────────────────────────
CREATE TABLE surgeries (
    id                TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id        TEXT NOT NULL,
    lead_surgeon_id   TEXT NOT NULL,
    anesthetist_id    TEXT NOT NULL,
    operating_room_id TEXT NOT NULL REFERENCES operating_rooms(id),
    procedure_name    TEXT NOT NULL,
    procedure_code    TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL DEFAULT 'SCHEDULED',
    scheduled_start   TIMESTAMP NOT NULL,
    scheduled_end     TIMESTAMP NOT NULL,
    actual_start      TIMESTAMP,
    actual_end        TIMESTAMP,
    pre_op_notes      TEXT NOT NULL DEFAULT '',
    post_op_notes     TEXT NOT NULL DEFAULT '',
    complications     TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP NOT NULL,

    -- VFKs
    patient_lsn INT,
    surgeon_lsn INT,
    anesth_lsn INT,
    VERSIONED FOREIGN KEY (patient_id)      REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (lead_surgeon_id) REFERENCES staff.staff(id)       AS OF surgeon_lsn,
    VERSIONED FOREIGN KEY (anesthetist_id)  REFERENCES staff.staff(id)       AS OF anesth_lsn
);

CREATE TABLE surgery_team_members (
    id         TEXT PRIMARY KEY DEFAULT UUID_V7,
    surgery_id TEXT NOT NULL REFERENCES surgeries(id),
    staff_id   TEXT NOT NULL,
    role       TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,

    staff_lsn INT,
    VERSIONED FOREIGN KEY (staff_id) REFERENCES staff.staff(id) AS OF staff_lsn
);

-- ── Wards & Beds ───────────────────────────────────────────
CREATE TABLE wards (
    id             TEXT PRIMARY KEY DEFAULT UUID_V7,
    name           TEXT NOT NULL,
    code           TEXT NOT NULL UNIQUE,
    department_id  TEXT NOT NULL DEFAULT '',
    floor          INT NOT NULL DEFAULT 0,
    building       TEXT NOT NULL DEFAULT '',
    total_beds     INT NOT NULL DEFAULT 0,
    active         BOOL NOT NULL DEFAULT true,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL,

    department_lsn INT,
    VERSIONED FOREIGN KEY (department_id) REFERENCES staff.departments(id) AS OF department_lsn
);

CREATE TABLE beds (
    id         TEXT PRIMARY KEY DEFAULT UUID_V7,
    ward_id    TEXT NOT NULL REFERENCES wards(id),
    number     TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'AVAILABLE',
    room_no    TEXT NOT NULL DEFAULT '',
    features   TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- ── Admissions ─────────────────────────────────────────────
CREATE TABLE admissions (
    id                  TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id          TEXT NOT NULL,
    admitting_doctor_id TEXT NOT NULL,
    bed_id              TEXT NOT NULL REFERENCES beds(id),
    department_id       TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'ADMITTED',
    admission_date      TIMESTAMP NOT NULL,
    discharge_date      TIMESTAMP,
    diagnosis           TEXT NOT NULL DEFAULT '',
    admission_reason    TEXT NOT NULL DEFAULT '',
    dietary_needs       TEXT NOT NULL DEFAULT '',
    notes               TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL,

    patient_lsn INT,
    doctor_lsn INT,
    VERSIONED FOREIGN KEY (patient_id)          REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (admitting_doctor_id) REFERENCES staff.staff(id)       AS OF doctor_lsn
);

CREATE INDEX idx_admissions_patient ON admissions(patient_id);
CREATE INDEX idx_admissions_status ON admissions(status);

CREATE TABLE meal_orders (
    id            TEXT PRIMARY KEY DEFAULT UUID_V7,
    admission_id  TEXT NOT NULL REFERENCES admissions(id),
    meal_type     TEXT NOT NULL,
    date          TIMESTAMP NOT NULL,
    menu          TEXT NOT NULL DEFAULT '',
    dietary_note  TEXT NOT NULL DEFAULT '',
    delivered     BOOL NOT NULL DEFAULT false,
    delivered_at  TIMESTAMP,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);

CREATE TABLE care_notes (
    id            TEXT PRIMARY KEY DEFAULT UUID_V7,
    admission_id  TEXT NOT NULL REFERENCES admissions(id),
    staff_id      TEXT NOT NULL,
    note_type     TEXT NOT NULL,
    content       TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL,

    staff_lsn INT,
    VERSIONED FOREIGN KEY (staff_id) REFERENCES staff.staff(id) AS OF staff_lsn
);

-- Entity: Admission is an aggregate root covering meals + notes
CREATE ENTITY admission_entity (ROOT admissions, INCLUDES meal_orders, care_notes);

COMMIT;
