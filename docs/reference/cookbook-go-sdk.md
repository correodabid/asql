# ASQL Go Cookbook

This cookbook provides practical Go-centric workflows for integrating ASQL into services.

For the broader adoption path, see [docs/getting-started/README.md](getting-started/README.md).

## Prerequisites

- ASQL server running on `127.0.0.1:5433`
- Go `1.24.x`

Start server:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

---

## Recipe 1: Schema init + first write

Initialize schema once and run the first explicit ASQL transaction over pgwire:

```go
conn, err := pool.Acquire(ctx)
if err != nil {
	log.Fatal(err)
}
defer conn.Release()

if _, err := conn.Exec(ctx, "BEGIN DOMAIN app"); err != nil {
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS app.users (id INT PRIMARY KEY, email TEXT)"); err != nil {
	_, _ = conn.Exec(ctx, "ROLLBACK")
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "INSERT INTO app.users (id, email) VALUES ($1, $2)", 1, "bootstrap@example.com"); err != nil {
	_, _ = conn.Exec(ctx, "ROLLBACK")
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
	log.Fatal(err)
}
```

Expected:
- schema initialization succeeds,
- transaction commit succeeds.

---

## Recipe 2: Cross-domain transaction lifecycle

Run a write while keeping cross-domain scope explicit:

```go
if _, err := conn.Exec(ctx, "BEGIN CROSS DOMAIN app, billing"); err != nil {
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "INSERT INTO app.users (id, email) VALUES ($1, $2)", 2, "second@example.com"); err != nil {
	_, _ = conn.Exec(ctx, "ROLLBACK")
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "INSERT INTO billing.invoices (id, user_id, total_cents) VALUES ($1, $2, $3)", "inv-2", 2, 1200); err != nil {
	_, _ = conn.Exec(ctx, "ROLLBACK")
	log.Fatal(err)
}
if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
	log.Fatal(err)
}
```

Expected:
- domain transaction committed,
- cross-domain scope is explicit in the service code.

---

## Recipe 3: Historical inspection from Go

Use the same pgwire connection for temporal inspection:

```go
var currentLSN int64
if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&currentLSN); err != nil {
	log.Fatal(err)
}

rows, err := conn.Query(ctx, "SELECT id, email FROM app.users AS OF LSN $1", currentLSN)
if err != nil {
	log.Fatal(err)
}
defer rows.Close()
```

Expected:
- the same connection path handles current and historical reads,
- application code does not need a separate read API shape for time-travel.

---

## Recipe 4: Temporal introspection helpers

Use the pgwire compatibility surface from Go to inspect temporal metadata:

```go
conn, err := pgx.Connect(ctx, "postgres://asql@127.0.0.1:5433/asql?sslmode=disable")
if err != nil {
	log.Fatal(err)
}
defer conn.Close(ctx)

var currentLSN int64
if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&currentLSN); err != nil {
	log.Fatal(err)
}

var rowLSN int64
if err := conn.QueryRow(ctx, "SELECT row_lsn('billing.invoices', '42')").Scan(&rowLSN); err != nil {
	log.Fatal(err)
}

var entityVersion int64
if err := conn.QueryRow(ctx, "SELECT entity_version('recipes', 'recipe_aggregate', 'recipe-1')").Scan(&entityVersion); err != nil {
	log.Fatal(err)
}

var entityHeadLSN int64
if err := conn.QueryRow(ctx, "SELECT entity_head_lsn('recipes', 'recipe_aggregate', 'recipe-1')").Scan(&entityHeadLSN); err != nil {
	log.Fatal(err)
}

var resolvedReference int64
if err := conn.QueryRow(ctx, "SELECT resolve_reference('recipes.master_recipes', '1')").Scan(&resolvedReference); err != nil {
	log.Fatal(err)
}
```

Use these helpers when you need:
- the current visible engine head,
- the latest visible row head for a specific primary key,
- the latest visible aggregate version,
- the commit LSN of the latest aggregate version,
- the exact token a versioned foreign key would capture for the current committed snapshot.

---

## Programmatic usage reference

See also:

- [docs/getting-started/09-go-sdk-and-integration.md](getting-started/09-go-sdk-and-integration.md)
- [hospitalapp/app.go](../hospitalapp/app.go)

The older example client remains useful as a low-level reference for engine-oriented operations, but pgwire should be the default integration path for new services.