# 12. First PostgreSQL-Oriented Service Flow on ASQL

This guide is for teams that already have a small PostgreSQL-oriented service
and want to prove the first real ASQL read/write workflow without pretending the
database is drop-in PostgreSQL.

Use this guide when all of these are true:

- the service already owns its SQL or can inspect emitted SQL,
- the first target workflow fits one explicit domain,
- the team wants pgwire-first application integration,
- and success means “one real request works end to end” rather than “the whole
  app migrated unchanged”.

Use it together with:

- [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md)
- [10-adoption-playbook.md](10-adoption-playbook.md)
- [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md)
- [../reference/postgres-app-sql-translation-guide-v1.md](../reference/postgres-app-sql-translation-guide-v1.md)
- [../migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md)

## What this guide proves

By the end of this guide, an existing PostgreSQL-shaped Go service will:

- connect to ASQL over pgwire,
- create one table in one explicit domain,
- execute one insert-focused write flow,
- execute one update flow,
- read the resulting row back,
- and inspect the same row through ASQL history tooling.

That is the current adoption wedge.
It is intentionally smaller than “full PostgreSQL compatibility”.

## Starting assumptions

Assume the original service looked roughly like this:

- Go service with `pgx` or `pgxpool`,
- one `users` table,
- ordinary `INSERT`, `UPDATE`, `SELECT` queries,
- hidden PostgreSQL transaction open via `BEGIN` or `db.Begin()`,
- no need yet for arrays, broad catalog discovery, or `UPDATE ... RETURNING`.

That shape is a good fit for the current narrow app-facing lane.

## Step 1 — start ASQL locally

In one terminal:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

In a second terminal:

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## Step 2 — create the first schema with explicit domain scope

In the shell:

```sql
BEGIN DOMAIN app;
CREATE TABLE app.users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  status TEXT NOT NULL
);
COMMIT;
```

The important difference from a PostgreSQL-first mental model is not only the
DDL. It is that the first write boundary is explicit from the start.

## Step 3 — translate the first three PostgreSQL assumptions

Do this before touching the service code:

| PostgreSQL-oriented assumption | First ASQL translation |
|---|---|
| `postgres://...` connection string | Keep pgwire, but start with `default_query_exec_mode=simple_protocol`. |
| hidden `BEGIN` / `db.Begin()` | Open work with `BEGIN DOMAIN app` on an acquired connection. |
| `UPDATE ... RETURNING` as a default habit | Split into `UPDATE`, then follow-up `SELECT` when the row shape is needed. |

Recommended connection string:

```text
postgres://asql@127.0.0.1:5433/asql?sslmode=disable&default_query_exec_mode=simple_protocol
```

## Step 4 — replace hidden transaction ownership with an explicit helper

Many PostgreSQL-oriented services start from code shaped like this:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)
```

For the first ASQL flow, replace that pattern with explicit domain-scoped SQL on
one acquired connection.

```go
package main

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
    ID     string
    Email  string
    Status string
}

func withDomainTx(ctx context.Context, pool *pgxpool.Pool, domain string, fn func(*pgxpool.Conn) error) error {
    conn, err := pool.Acquire(ctx)
    if err != nil {
        return err
    }
    defer conn.Release()

    if _, err := conn.Exec(ctx, fmt.Sprintf("BEGIN DOMAIN %s", domain)); err != nil {
        return err
    }

    committed := false
    defer func() {
        if !committed {
            _, _ = conn.Exec(ctx, "ROLLBACK")
        }
    }()

    if err := fn(conn); err != nil {
        return err
    }
    if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
        return err
    }
    committed = true
    return nil
}
```

That helper keeps the first migration rule visible: transaction scope is part
of the application contract.

## Step 5 — prove one real write flow

Use one insert-focused workflow first.

```go
func createUser(ctx context.Context, pool *pgxpool.Pool, user User) (User, error) {
    var created User

    err := withDomainTx(ctx, pool, "app", func(conn *pgxpool.Conn) error {
        return conn.QueryRow(
            ctx,
            `INSERT INTO app.users (id, email, status)
             VALUES ($1, $2, $3)
             RETURNING id, email, status`,
            user.ID,
            user.Email,
            user.Status,
        ).Scan(&created.ID, &created.Email, &created.Status)
    })
    if err != nil {
        return User{}, err
    }

    return created, nil
}
```

This keeps the current documented `INSERT ... RETURNING` path, which is already
regression-covered for the ORM-lite lane.

## Step 6 — prove one update and one read flow

For update-heavy handlers, avoid assuming `UPDATE ... RETURNING`.

```go
func setUserStatus(ctx context.Context, pool *pgxpool.Pool, id, status string) error {
    return withDomainTx(ctx, pool, "app", func(conn *pgxpool.Conn) error {
        _, err := conn.Exec(
            ctx,
            `UPDATE app.users
             SET status = $2
             WHERE id = $1`,
            id,
            status,
        )
        return err
    })
}

func loadUser(ctx context.Context, pool *pgxpool.Pool, id string) (User, error) {
    var user User
    err := pool.QueryRow(
        ctx,
        `SELECT id, email, status
         FROM app.users
         WHERE id = $1
         ORDER BY id
         LIMIT 1`,
        id,
    ).Scan(&user.ID, &user.Email, &user.Status)
    if err != nil {
        return User{}, err
    }
    return user, nil
}
```

The first end-to-end proof can now be as small as:

```go
created, err := createUser(ctx, pool, User{
    ID:     "user-1",
    Email:  "alice@example.com",
    Status: "pending",
})
if err != nil {
    return err
}

if err := setUserStatus(ctx, pool, created.ID, "active"); err != nil {
    return err
}

loaded, err := loadUser(ctx, pool, created.ID)
if err != nil {
    return err
}

fmt.Printf("user=%+v\n", loaded)
```

Success means the service can:

- create the row,
- mutate the row,
- read the row back over pgwire,
- and do it with explicit ASQL transaction scope.

## Step 7 — use one ASQL-native inspection immediately

Once the first request works, do not stop at “the row exists”.
Prove one ASQL differentiator right away.

In the shell:

```sql
SELECT current_lsn();

SELECT id, email, status, __operation, __commit_lsn
FROM app.users FOR HISTORY
WHERE id = 'user-1';
```

That gives the team an immediate before/after audit trail for the same workflow
it just moved.

## Step 8 — know what not to port in the first wave

Do not make the first success criteria too broad.

Defer these until after the basic flow is green:

- broad ORM metadata discovery,
- `UPDATE ... RETURNING` and `DELETE ... RETURNING`,
- arrays and `ANY(...)`,
- builder-mode BI workflows,
- cross-domain writes unless the atomic boundary is already clear,
- full PostgreSQL parity assumptions in migrations or test helpers.

The first migration wedge is intentionally narrow so the team can separate real
engine/model fit from abstraction noise.

For the next PostgreSQL-shaped query rewrites after this first workflow is
green, continue with [../reference/postgres-app-sql-translation-guide-v1.md](../reference/postgres-app-sql-translation-guide-v1.md).

## Step 9 — recommended proof checklist for the app repository

Before calling this first workflow successful, record these artifacts in the app
repository:

- the exact pgwire connection string,
- the explicit transaction helper,
- one schema bootstrap file,
- one insert/update/read happy-path test,
- one note listing translated PostgreSQL assumptions,
- one example of `FOR HISTORY` or `AS OF LSN` used on the migrated row.

That bundle is usually enough to decide whether the app should continue with the
current ASQL wedge.

## What to do next

After this guide, choose the next step intentionally:

- if the service still fits the narrow subset, continue with [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md),
- if the app is still closer to SQLite/Postgres-lite migration planning, use [../migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md),
- if the team wants broader rollout sequencing, use [10-adoption-playbook.md](10-adoption-playbook.md),
- if the next friction is driver behavior, use [../reference/pgwire-driver-guidance-v1.md](../reference/pgwire-driver-guidance-v1.md),
- if the next friction is debugging or history, continue with [05-time-travel-and-history.md](05-time-travel-and-history.md).