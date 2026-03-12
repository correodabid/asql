-- Hospital MiKS – Domain: identity
-- Auth users isolated in their own domain.

BEGIN DOMAIN identity;

CREATE TABLE users (
    id             TEXT PRIMARY KEY DEFAULT UUID_V7,
    staff_id       TEXT NOT NULL,
    username       TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL,
    active         BOOL NOT NULL DEFAULT true,
    last_login_at  TIMESTAMP,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE INDEX idx_users_staff_id ON users(staff_id);

COMMIT;
