# 09. Go SDK and Integration

ASQL is implemented in Go, so the most natural application integration path is through Go services and tools.

## Start from the example

Reference implementation:

- [../../examples/go-client/main.go](../../examples/go-client/main.go)
- [../cookbook-go-sdk.md](../cookbook-go-sdk.md)

## Typical integration shape

A Go service usually needs these operations:

- begin transaction,
- execute statements,
- commit or rollback,
- run historical queries when needed,
- optionally call temporal helpers over pgwire.

## Minimal patterns

### First schema and write

Use the example client to bootstrap the shape of your integration.

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -init-schema -id 1 -email bootstrap@example.com
```

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

## Recommended integration strategy

- keep transaction orchestration explicit,
- wrap domain names in small application helpers,
- add fixtures early for integration tests,
- reserve advanced temporal logic for places that truly need it.

## Next step

Continue with [10-adoption-playbook.md](10-adoption-playbook.md).
