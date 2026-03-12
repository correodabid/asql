-- Hospital MiKS – Domain: staff
-- Staff members and departments.

BEGIN DOMAIN staff;

CREATE TABLE departments (
    id          TEXT PRIMARY KEY DEFAULT UUID_V7,
    name        TEXT NOT NULL,
    code        TEXT NOT NULL UNIQUE,
    floor       INT NOT NULL DEFAULT 0,
    building    TEXT NOT NULL DEFAULT '',
    head_id     TEXT,
    active      BOOL NOT NULL DEFAULT true,
    created_at  TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE staff (
    id              TEXT PRIMARY KEY DEFAULT UUID_V7,
    employee_code   TEXT NOT NULL UNIQUE,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    email           TEXT NOT NULL UNIQUE,
    phone           TEXT NOT NULL DEFAULT '',
    staff_type      TEXT NOT NULL,
    specialty       TEXT NOT NULL DEFAULT '',
    license_number  TEXT NOT NULL DEFAULT '',
    department_id   TEXT REFERENCES departments(id),
    hire_date       TIMESTAMP NOT NULL,
    active          BOOL NOT NULL DEFAULT true,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE INDEX idx_staff_department ON staff(department_id);
CREATE INDEX idx_staff_type ON staff(staff_type);

-- Entity: a Staff member is the aggregate root
CREATE ENTITY staff_entity (ROOT staff);

COMMIT;
