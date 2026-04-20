# 09. Go SDK and Integration

ASQL is implemented in Go, so the most natural application integration path is through Go services and tools.

For ordinary application reads and writes, prefer pgwire with `pgx` or `pgxpool`.
Use lower-level gRPC tooling only when you intentionally need engine-level administrative flows.

For driver/query-mode guidance, see [../reference/pgwire-driver-guidance-v1.md](../reference/pgwire-driver-guidance-v1.md).
For the narrow app-facing translation lane that works today with PostgreSQL-oriented services, see [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md).
For one end-to-end "existing PostgreSQL-oriented service reaches its first successful read/write flow" guide, see [12-first-postgres-service-flow.md](12-first-postgres-service-flow.md).

## Start from the reference

Reference material:

- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [`sdk/`](../../sdk/) — typed Go client on top of the gRPC admin surface.

When the durable principal catalog is enabled, the gRPC admin/query surface now
expects database-principal metadata on calls that open transactions or inspect
current/historical engine state. Clients send `asql-principal` and
`asql-password` via metadata; bearer tokens remain separate transport/operator
controls.

Important: the default local runtime started by `go run ./cmd/asqld ...` in
getting-started exposes pgwire on `:5433`. The gRPC admin surface is not the
primary application-facing path for new teams.

For new application code, a pgwire-first service shape is the right default.
That is also the runtime started in [02-install-and-run.md](02-install-and-run.md).

## Typical integration shape

A Go service usually needs these operations:

- begin explicit domain-scoped work,
- execute statements over pgwire,
- commit or rollback,
- run historical queries when needed,
- call temporal helpers over pgwire.

## Minimal patterns

### Explicit ASQL transaction orchestration

This is the application-facing pattern used in real services.

```go
conn, err := pool.Acquire(ctx)
if err != nil {
	return err
}
defer conn.Release()

if _, err := conn.Exec(ctx, "BEGIN DOMAIN app"); err != nil {
	return err
}
if _, err := conn.Exec(ctx, "INSERT INTO app.users (id, email) VALUES ($1, $2)", 1, "bootstrap@example.com"); err != nil {
	_, _ = conn.Exec(ctx, "ROLLBACK")
	return err
}
if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
	return err
}
```

Wrap the domain list in a small helper so the application does not scatter boundary decisions across handlers.

### Temporal helper queries from Go

Use pgwire and `pgx` when you need helper lookups.

```go
var currentLSN int64
_ = conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&currentLSN)

var rowLSN int64
_ = conn.QueryRow(ctx, "SELECT row_lsn('app.users', '1')").Scan(&rowLSN)
```

### Aggregate-oriented lookups

```go
var version int64
_ = conn.QueryRow(ctx, "SELECT entity_version('billing', 'invoice_aggregate', 'inv-1')").Scan(&version)

var versionLSN int64
_ = conn.QueryRow(ctx, "SELECT entity_version_lsn('billing', 'invoice_aggregate', 'inv-1', 3)").Scan(&versionLSN)
```

Use `entity_version_lsn(...)` when your application thinks in versions but the read path needs `AS OF LSN`.

### Snapshot reads from Go

```go
rows, err := pool.Query(ctx, "SELECT id, email FROM app.users AS OF LSN $1", 4)
if err != nil {
	return err
}
defer rows.Close()
```

This keeps the historical read path in normal SQL instead of introducing a separate application API shape.

## Recommended integration strategy

- keep transaction orchestration explicit,
- wrap domain names in small application helpers,
- add fixtures early for integration tests,
- reserve advanced temporal logic for places that truly need it.

## Recommended pgwire path for new teams

Use this as the lowest-surprise baseline:

- `pgx` or `pgxpool`,
- explicit SQL over pgwire,
- `sslmode=disable` for local development or `sslmode=prefer` for PostgreSQL-oriented tooling,
- `default_query_exec_mode=simple_protocol` while the team is still validating compatibility and model assumptions.

ASQL supports the extended query pipeline for the documented subset.
Still, simple protocol is the recommended first adoption path because it makes compatibility issues easier to reproduce and reason about.

For a reusable app-side baseline, use the compact starter conventions in [10-adoption-playbook.md](10-adoption-playbook.md).

If the team is evaluating a light query builder or ORM-like layer, keep the first proof narrow:

- pin `default_query_exec_mode=simple_protocol`,
- keep `BEGIN DOMAIN ...` or `BEGIN CROSS DOMAIN ...` explicit,
- use `INSERT ... RETURNING` only on the documented insert path,
- use plain `UPDATE` / `DELETE` plus a follow-up `SELECT` when the app needs post-mutation state,
- inspect emitted SQL instead of assuming broad PostgreSQL parity.

## Common expectation mismatch

Do not expect ASQL to remove application workflow code.
The service still decides:

- which domains participate,
- which business steps belong in one transaction,
- what audit payloads mean,
- what IDs and timestamps are used.

ASQL gives that code a more explicit and replay-safe substrate.

## Next step

Continue with [10-adoption-playbook.md](10-adoption-playbook.md).
