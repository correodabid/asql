# Migration Guide v1: SQLite/Postgres-lite to ASQL

Date: 2026-03-01
Audience: teams migrating lightweight SQL workloads to deterministic ASQL operations.

## 1) When this guide applies

Best-fit migrations:
- embedded or small service backends using SQLite,
- PostgreSQL-lite usage patterns (basic DDL/DML, low join complexity, moderate concurrency),
- products needing deterministic replay, time-travel reads, and stronger auditability.

Not in scope for direct lift-and-shift in one step:
- broad PostgreSQL advanced feature usage (window functions, deep procedural SQL, broad extension dependencies),
- highly optimized complex query plans that require full PostgreSQL optimizer parity.

## 2) Migration strategy (recommended)

Use a phased approach to reduce risk:

1. **Discover** current SQL surface and operational constraints.
2. **Align schema** to ASQL-supported SQL subset.
3. **Move write path** to explicit ASQL transaction flow.
4. **Backfill and verify** with deterministic replay/time-travel checks.
5. **Cut over** with rollback plan and compatibility checks.

## 3) Feature compatibility checklist

Before migration, inventory and classify current usage:
- DDL: tables, PK/UNIQUE/FK/CHECK constraints.
- DML: `INSERT`, `UPDATE`, `DELETE`, selective `SELECT` paths.
- Transactions: single-domain vs cross-domain behavior.
- Query complexity: joins, aggregates, boolean/arithmetic expressions.
- Operational needs: replay, historical reads, replication mode.

Then map to ASQL capabilities and mark each path as:
- **Ready now**,
- **Needs query rewrite**,
- **Deferred**.

## 4) Schema and type mapping baseline

Typical starter mapping:
- `INTEGER` -> `INT`
- `TEXT` -> `TEXT`

Example:

SQLite/Postgres-lite style:

```sql
CREATE TABLE users (id INTEGER, email TEXT);
```

ASQL:

```sql
CREATE TABLE users (id INT, email TEXT);
```

If migrating multi-domain workloads, split schema by explicit domain boundaries early.

## 5) Transaction model migration

ASQL requires explicit transaction scope at begin time.

Single domain:

```sql
BEGIN DOMAIN app;
-- statements
COMMIT;
```

Cross-domain:

```sql
BEGIN CROSS DOMAIN app, app_aux;
-- statements over declared domains only
COMMIT;
```

Application write paths should move from implicit DB transactions to API flow:
- `BeginTx` -> `Execute` -> `CommitTx` (or `RollbackTx`).

For new application code, prefer pgwire SQL with explicit `BEGIN DOMAIN ...` or `BEGIN CROSS DOMAIN ...` statements over low-level client orchestration.

## 6) Practical migration runbook

### Step A — bootstrap ASQL and schema

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

Then in the shell:

```sql
BEGIN DOMAIN app;
CREATE TABLE app.users (id INT PRIMARY KEY, email TEXT);
COMMIT;
```

### Step B — redirect a pilot write path

```sql
BEGIN DOMAIN app;
INSERT INTO app.users (id, email) VALUES (2, 'migrated@example.com');
COMMIT;
```

### Step C — validate admin/read-history behavior

```sql
SELECT current_lsn();
SELECT * FROM app.users FOR HISTORY WHERE id = 2;
SELECT * FROM app.users AS OF LSN 1;
```

### Step D — migration preflight and rollback rehearsal (required)

Before applying production migration SQL:

1. Prepare explicit forward and rollback SQL scripts (same domain scope).
2. Ensure rollback SQL is truly state-restoring (not only syntactically valid).
3. Rehearse both scripts in staging with representative data volume.

Recommended migration shape for online-safe rollout (current ASQL subset):

Forward:

```sql
BEGIN DOMAIN app;
ALTER TABLE users ADD COLUMN email TEXT;
COMMIT;
```

Rollback (for the same window):

```sql
BEGIN DOMAIN app;
UPDATE users SET email = NULL;
COMMIT;
```

Notes:
- Prefer additive schema changes first (`ADD COLUMN`) and postpone destructive changes.
- Keep rollback scripts deterministic and idempotency-aware for the migration window.
- If rollback cannot be guaranteed at SQL level, require WAL backup restore rollback as primary path.

### Step E — deterministic verification gates before cutover

Run these checks after forward rehearsal and after rollback rehearsal:

1. Row parity checks (counts + key queries) on affected tables.
2. Replay/time-travel parity checks on migrated domain(s).
3. Backup/restore query parity check.

Suggested commands:

```bash
go test ./test/integration -run TestReplayToLSNAndTimeTravelQueries -v
go test ./test/integration -run TestBackupWipeRestorePreservesQueryParity -v
```

Promotion rule:
- Do not proceed to production cutover unless forward + rollback rehearsals both satisfy deterministic parity gates.

## 7) Data backfill and parity checks

For existing datasets:
- export source rows in deterministic order,
- import into ASQL in stable batches,
- compare row counts and key sample queries.

Validation gates before cutover:
- deterministic replay checks pass,
- historical read checks (`AS OF LSN` / timestamp) pass,
- backup/restore query parity checks pass.

## 8) Cutover plan

Recommended production cutover steps:
1. Freeze source writes.
2. Run final incremental sync.
3. Execute parity verification suite.
4. Switch application write/read endpoint to ASQL.
5. Monitor latency, errors, and replication/replay health.

Rollback readiness:
- keep source snapshot + ASQL WAL backup,
- define explicit rollback window and owner,
- pre-approve rollback trigger thresholds (error rate, parity mismatch, replay failures),
- keep tested rollback SQL script and WAL restore procedure side-by-side in release artifacts.

Rollback execution path (recommended order):
1. Stop new writes to migrated path.
2. Execute rehearsed rollback SQL script (if declared rollback-safe for current window).
3. Re-run deterministic verification gates.
4. If parity still fails, restore ASQL from pre-cutover WAL backup and route traffic to source snapshot.
5. Record incident timeline + migration diff evidence for postmortem.

## 9) Common migration pitfalls

- Assuming implicit cross-domain side effects instead of explicit transaction scope.
- Porting unsupported SQL constructs without rewrite.
- Skipping replay/time-travel verification prior to cutover.
- Comparing benchmark numbers across non-equivalent hardware/settings.

If the team is still carrying SQLite/Postgres/ORM assumptions into the migration, read the concise FAQ in [../getting-started/10-adoption-playbook.md](../getting-started/10-adoption-playbook.md).

## 10) Companion references

- `docs/migration/sqlite-quick-path.md`
- `docs/reference/cookbook-go-sdk.md`
- `docs/operations/release-upgrade-compat-checklist-v1.md`
- `docs/operations/runbook.md`
- `docs/product/benchmark-one-pager-v1.md`
