# PostgreSQL-Oriented App SQL Translation Guide v1

This guide turns the current ASQL compatibility wedge into an app-facing SQL
translation checklist for teams trialing an existing PostgreSQL-oriented
service.

Use it together with:

- [pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [orm-lite-adoption-lane-v1.md](orm-lite-adoption-lane-v1.md)
- [../getting-started/12-first-postgres-service-flow.md](../getting-started/12-first-postgres-service-flow.md)

Purpose:

- show which mainstream PostgreSQL-shaped SQL patterns are already a good fit
  for first evaluation,
- show which patterns need explicit ASQL translation,
- and stop teams from treating one working pgwire connection as proof of broad
  PostgreSQL parity.

## How to use this guide

When evaluating an existing service, classify each SQL shape into one of three
buckets:

- **use now** — already inside the documented ASQL subset,
- **translate first** — workable after a small explicit rewrite,
- **defer** — not part of the current adoption wedge.

The goal is not to convert every query immediately.
The goal is to get one real read/write flow green with the smallest safe subset.

## Use now: good first-evaluation SQL shapes

These shapes already have a relatively strong adoption story in the documented
subset.

| PostgreSQL-shaped SQL shape | Current ASQL stance | Notes |
|---|---|---|
| `SELECT ... WHERE <scalar predicate>` | Use now | Strong baseline query shape over pgwire. |
| `ORDER BY ... LIMIT ...` and `LIMIT ... OFFSET ...` | Use now | Supported; keyset pagination is still preferable for large scans. |
| Literal `IN (...)` / `NOT IN (...)` | Use now | Good replacement for many small array-style filters. |
| `IN (SELECT ...)`, `EXISTS`, `NOT EXISTS` | Use now | Supported for current documented shapes. |
| `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` | Use now | Good for current supported join shapes; keep query shapes inspectable. |
| `ILIKE` / `NOT ILIKE` | Use now | Useful for first search/filter adoption loops. |
| Simple non-recursive `WITH` / CTE shapes | Use now | Supported in the current documented subset. |
| `INSERT ... RETURNING ...` | Use now | Current `RETURNING` support is insert-focused. |
| `INSERT ... ON CONFLICT ...` | Use now | Good default upsert path for the currently documented shapes. |

For first serious evaluation, these shapes are better targets than broad ORM
metadata discovery or PostgreSQL-specific type work.

## Translate first: high-return rewrites before trial

These are the PostgreSQL assumptions most likely to break an otherwise simple
service during the first ASQL trial.

| PostgreSQL-shaped assumption | Translate to | Why |
|---|---|---|
| `BEGIN` / `START TRANSACTION` | `BEGIN DOMAIN <name>` or `BEGIN CROSS DOMAIN <a>, <b>` | ASQL transaction scope is explicit and domain-aware. |
| hidden `db.Begin()` / ORM-owned transaction open | one explicit app helper that acquires a connection and sends `BEGIN DOMAIN ...` | Keeps transaction ownership visible in the service layer. |
| `UPDATE ... RETURNING ...` | `UPDATE ...` then follow-up `SELECT ... WHERE pk = $1` | Current `RETURNING` support is not documented beyond `INSERT`. |
| `DELETE ... RETURNING ...` | `SELECT ...` first if row shape is needed, then `DELETE ...` | Keeps deletion and row inspection explicit. |
| `ANY(ARRAY[...])` predicates | literal `IN (...)`, `IN (SELECT ...)`, or remodel the filter input | Arrays are outside the current ASQL subset. |
| broad PostgreSQL role SQL (`CREATE ROLE`, `GRANT`, `REVOKE`) | `asqlctl`, Studio security flows, or admin API | Durable principals are supported, but not via PostgreSQL role SQL. |

### Rewrite examples

#### 1. Transaction open

PostgreSQL-oriented shape:

```sql
BEGIN;
UPDATE users SET status = 'active' WHERE id = 'user-1';
COMMIT;
```

ASQL path:

```sql
BEGIN DOMAIN app;
UPDATE app.users SET status = 'active' WHERE id = 'user-1';
COMMIT;
```

#### 2. Update with row fetch

PostgreSQL-oriented shape:

```sql
UPDATE users
SET status = 'active'
WHERE id = $1
RETURNING id, email, status;
```

ASQL path:

```sql
UPDATE app.users
SET status = 'active'
WHERE id = $1;

SELECT id, email, status
FROM app.users
WHERE id = $1;
```

#### 3. Array-style filter

PostgreSQL-oriented shape:

```sql
SELECT id, email
FROM users
WHERE id = ANY($1);
```

ASQL path for small explicit sets:

```sql
SELECT id, email
FROM app.users
WHERE id IN ('user-1', 'user-2', 'user-3');
```

ASQL path when the filter already exists as rows:

```sql
SELECT u.id, u.email
FROM app.users u
WHERE u.id IN (
  SELECT requested_user_id
  FROM app.requested_users
);
```

## Defer: not part of the current app-adoption wedge

Treat these as out of scope for the first success loop:

- broad ORM metadata/discovery behavior,
- full PostgreSQL catalog parity,
- PostgreSQL arrays and array operators,
- broad prepared-statement parity beyond the documented session-scoped path,
- generic proof that an arbitrary ORM or framework works unchanged,
- PostgreSQL role-management SQL as the management surface for ASQL principals.

If one of these appears early, do not widen the claim casually.
Either translate around it, or explicitly mark it as a deferred compatibility
question.

## Recommended evaluation order

1. get one read path green with scalar predicates,
2. get one write path green with `INSERT ... RETURNING ...`,
3. translate transaction ownership to `BEGIN DOMAIN ...`,
4. translate one update path away from `UPDATE ... RETURNING ...`,
5. only then evaluate whether the next friction is SQL, metadata, or the app abstraction layer.

## Quick checklist for a PostgreSQL-oriented service repo

Before calling the SQL layer "trial-ready", confirm that the repository now has:

- one explicit ASQL transaction helper,
- one insert path using documented `INSERT ... RETURNING ...`,
- one update path rewritten without `UPDATE ... RETURNING ...`,
- no remaining dependency on arrays / `ANY(...)` in the first workflow,
- one note listing every translated PostgreSQL assumption,
- and one validated read-back query after commit.

That is enough for a credible first ASQL application trial.