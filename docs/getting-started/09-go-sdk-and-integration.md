# 09. Go SDK and Integration

ASQL is implemented in Go, so the most natural application integration path is through Go services and tools.

For ordinary application reads and writes, prefer pgwire with `pgx` or `pgxpool`.
Use lower-level gRPC tooling only when you intentionally need engine-level administrative flows.

## Start from the example

Reference implementation:

- [../../examples/go-client/main.go](../../examples/go-client/main.go)
- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)

Treat the example client as a low-level reference for engine operations.
For new application code, a pgwire-first service shape is usually the right default.

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

For a reusable app-side baseline, continue with [09a-general-purpose-starter-pack.md](09a-general-purpose-starter-pack.md).

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
