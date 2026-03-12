-- Hospital MiKS – Domain: documents
-- Document management and access audit log.

BEGIN DOMAIN documents;

CREATE TABLE documents (
    id           TEXT PRIMARY KEY DEFAULT UUID_V7,
    title        TEXT NOT NULL,
    category     TEXT NOT NULL,
    patient_id   TEXT NOT NULL DEFAULT '',
    uploaded_by  TEXT NOT NULL,
    file_name    TEXT NOT NULL,
    mime_type    TEXT NOT NULL DEFAULT '',
    size_bytes   INT NOT NULL DEFAULT 0,
    storage_path TEXT NOT NULL DEFAULT '',
    checksum     TEXT NOT NULL DEFAULT '',
    version      INT NOT NULL DEFAULT 1,
    tags         TEXT NOT NULL DEFAULT '',
    notes        TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL,

    -- VFKs
    patient_lsn INT,
    uploader_lsn INT,
    VERSIONED FOREIGN KEY (patient_id)  REFERENCES patients.patients(id) AS OF patient_lsn,
    VERSIONED FOREIGN KEY (uploaded_by) REFERENCES staff.staff(id)       AS OF uploader_lsn
);

CREATE INDEX idx_documents_patient ON documents(patient_id);
CREATE INDEX idx_documents_category ON documents(category);

CREATE TABLE document_access_log (
    id          TEXT PRIMARY KEY DEFAULT UUID_V7,
    document_id TEXT NOT NULL REFERENCES documents(id),
    staff_id    TEXT NOT NULL,
    action      TEXT NOT NULL,
    ip_address  TEXT NOT NULL DEFAULT '',
    accessed_at TIMESTAMP NOT NULL,

    staff_lsn INT,
    VERSIONED FOREIGN KEY (staff_id) REFERENCES staff.staff(id) AS OF staff_lsn
);

-- Entity: Document aggregate
CREATE ENTITY document_entity (ROOT documents, INCLUDES document_access_log);

COMMIT;
