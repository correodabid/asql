-- Hospital MiKS – Domain: messaging
-- Internal staff messages and patient communications.

BEGIN DOMAIN messaging;

CREATE TABLE messages (
    id          TEXT PRIMARY KEY DEFAULT UUID_V7,
    sender_id   TEXT NOT NULL,
    receiver_id TEXT NOT NULL,
    subject     TEXT NOT NULL,
    body        TEXT NOT NULL,
    priority    TEXT NOT NULL DEFAULT 'NORMAL',
    read        BOOL NOT NULL DEFAULT false,
    read_at     TIMESTAMP,
    parent_id   TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMP NOT NULL,

    -- VFKs
    sender_lsn INT,
    receiver_lsn INT,
    VERSIONED FOREIGN KEY (sender_id)   REFERENCES staff.staff(id) AS OF sender_lsn,
    VERSIONED FOREIGN KEY (receiver_id) REFERENCES staff.staff(id) AS OF receiver_lsn
);

CREATE INDEX idx_messages_receiver ON messages(receiver_id);
CREATE INDEX idx_messages_sender ON messages(sender_id);

CREATE TABLE patient_communications (
    id           TEXT PRIMARY KEY DEFAULT UUID_V7,
    patient_id   TEXT NOT NULL,
    staff_id     TEXT NOT NULL,
    channel      TEXT NOT NULL,
    subject      TEXT NOT NULL DEFAULT '',
    content      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'SENT',
    sent_at      TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP,
    created_at   TIMESTAMP NOT NULL,

    patient_lsn INT,
    staff_lsn INT,
    VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (staff_id)   REFERENCES staff.staff(id)       AS OF staff_lsn
);

CREATE INDEX idx_patient_comms_patient ON patient_communications(patient_id);

COMMIT;
