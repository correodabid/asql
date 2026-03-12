-- Hospital MiKS – Domain: scheduling
-- Guard shifts.

BEGIN DOMAIN scheduling;

CREATE TABLE guard_shifts (
    id             TEXT PRIMARY KEY DEFAULT UUID_V7,
    staff_id       TEXT NOT NULL,
    department_id  TEXT NOT NULL,
    type           TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'SCHEDULED',
    start_time     TIMESTAMP NOT NULL,
    end_time       TIMESTAMP NOT NULL,
    notes          TEXT NOT NULL DEFAULT '',
    swapped_with   TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL,

    -- VFKs
    staff_lsn INT,
    department_lsn INT,
    VERSIONED FOREIGN KEY (staff_id)       REFERENCES staff.staff(id)        AS OF staff_lsn,
    VERSIONED FOREIGN KEY (department_id)  REFERENCES staff.departments(id)  AS OF department_lsn
);

CREATE INDEX idx_guard_shifts_staff ON guard_shifts(staff_id);
CREATE INDEX idx_guard_shifts_department ON guard_shifts(department_id);
CREATE INDEX idx_guard_shifts_start ON guard_shifts(start_time);

COMMIT;
