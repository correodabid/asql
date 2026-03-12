# SQLite to ASQL Quick Migration Path

This guide helps teams move lightweight SQLite-style workloads to ASQL quickly.

For the full adoption guide, see [docs/getting-started/10-adoption-playbook.md](getting-started/10-adoption-playbook.md).

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
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -init-schema -id 1 -email bootstrap@example.com
```

## 3) Move write path

Replace direct SQLite writes with ASQL transaction flow (`BeginTx -> Execute -> CommitTx`).

Reference command:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -id 2 -email migrated@example.com
```

## 4) Add audit/replay checks

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -id 3 -email replay@example.com -verify-admin
```

## Caveats

- ASQL currently targets a focused SQL subset; verify unsupported SQLite constructs before migration.
- Avoid relying on implicit SQLite behavior not explicitly represented in ASQL transaction flow.
- For production migration, validate deterministic replay on representative data before cutover.