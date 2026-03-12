# SQLite to ASQL Quick Migration Path

This guide helps teams move lightweight SQLite-style workloads to ASQL quickly.

This is the short migration companion.

For the fuller migration guide, see [docs/migration/sqlite-postgres-lite-guide-v1.md](sqlite-postgres-lite-guide-v1.md).
For the broader adoption guide, see [docs/getting-started/10-adoption-playbook.md](../getting-started/10-adoption-playbook.md).

## Scope

Best fit:
- single-writer or low-contention backends,
- audit/replay-sensitive workflows,
- edge/offline-first services.

## 1) Map schema

Typical mapping:
- `INTEGER` -> `INT`
- `TEXT` -> `TEXT`

Example:

SQLite:

```sql
CREATE TABLE users (id INTEGER, email TEXT);
```

ASQL:

```sql
CREATE TABLE users (id INT, email TEXT);
```

## 2) Initialize ASQL schema

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

Then create the schema explicitly:

```sql
BEGIN DOMAIN app;
CREATE TABLE app.users (id INT PRIMARY KEY, email TEXT);
COMMIT;
```

## 3) Move write path

Replace direct SQLite writes with explicit ASQL transaction flow.

```sql
BEGIN DOMAIN app;
INSERT INTO app.users (id, email) VALUES (2, 'migrated@example.com');
COMMIT;
```

## 4) Add audit/replay checks

```sql
SELECT current_lsn();
SELECT * FROM app.users FOR HISTORY WHERE id = 2;
SELECT * FROM app.users AS OF LSN 1;
```

## Caveats

- ASQL currently targets a focused SQL subset; verify unsupported SQLite constructs before migration.
- Avoid relying on implicit SQLite behavior not explicitly represented in ASQL transaction flow.
- For production migration, validate deterministic replay on representative data before cutover.
- Prefer fixtures for migration rehearsals instead of ad hoc seed scripts.