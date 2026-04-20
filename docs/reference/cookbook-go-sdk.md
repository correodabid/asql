# ASQL Go Cookbook

This cookbook provides practical Go-centric workflows for integrating ASQL into services.

For the broader adoption path, see [docs/getting-started/README.md](../getting-started/README.md).

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

### Helper pattern: keep the boundary explicit, not duplicated

For real services, wrap the begin/commit/rollback boilerplate without hiding the domain choice itself.

```go
func RunDomainTx(ctx context.Context, conn *pgx.Conn, domain string, fn func(*pgx.Conn) error) error {
	return runScopedTx(ctx, conn, "BEGIN DOMAIN "+domain, fn)
}

func RunCrossDomainTx(ctx context.Context, conn *pgx.Conn, domains []string, fn func(*pgx.Conn) error) error {
	return runScopedTx(ctx, conn, "BEGIN CROSS DOMAIN "+strings.Join(domains, ", "), fn)
}

func runScopedTx(ctx context.Context, conn *pgx.Conn, beginSQL string, fn func(*pgx.Conn) error) error {
	if _, err := conn.Exec(ctx, beginSQL); err != nil {
		return err
	}
	if err := fn(conn); err != nil {
		_, _ = conn.Exec(ctx, "ROLLBACK")
		return err
	}
	_, err := conn.Exec(ctx, "COMMIT")
	return err
}
```

This pattern keeps three things true at the same time:

- the transaction boundary stays explicit in application code,
- repeated boilerplate does not spread across handlers,
- rollback behavior stays consistent.

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

### Helper pattern: snapshot lookup

Wrap the `AS OF LSN` read path in a small helper when the service needs repeatable snapshot reads.

```go
func QuerySnapshot(ctx context.Context, conn *pgx.Conn, sql string, lsn int64, args ...any) (pgx.Rows, error) {
	snapshotSQL := sql + " AS OF LSN $1"
	snapshotArgs := append([]any{lsn}, args...)
	return conn.Query(ctx, snapshotSQL, snapshotArgs...)
}
```

Use this when the application already knows the replay-safe `LSN` it wants to inspect.

### Helper pattern: history lookup

Wrap `FOR HISTORY` when you want one reusable path for row mutation trails.

```go
func QueryHistory(ctx context.Context, conn *pgx.Conn, table string, pk any) (pgx.Rows, error) {
	sql := fmt.Sprintf("SELECT * FROM %s FOR HISTORY WHERE id = $1", table)
	return conn.Query(ctx, sql, pk)
}
```

Use this when the service needs to render or analyze the chronological mutation trail for one business row.

### Helper pattern: version-to-LSN bridge

Use a helper like this when the application thinks in entity versions but the read path needs `AS OF LSN`.

```go
func LookupEntityVersionLSN(ctx context.Context, conn *pgx.Conn, domain, entity, rootPK string, version int64) (int64, error) {
	var lsn int64
	err := conn.QueryRow(
		ctx,
		"SELECT entity_version_lsn($1, $2, $3, $4)",
		domain,
		entity,
		rootPK,
		version,
	).Scan(&lsn)
	return lsn, err
}
```

This is the clean bridge between aggregate-oriented service logic and replay-safe historical reads.

### Compose the helpers into one explanation flow

The most reusable temporal service pattern is:

1. fetch current row or entity state,
2. fetch history,
3. resolve the target `LSN`,
4. fetch the snapshot at that `LSN`.

That keeps temporal logic explicit without inventing a separate application API shape just for history.

### Helper pattern: current -> history -> snapshot explanation

Wrap the full workflow when the service repeatedly needs one explainable historical read path.

```go
type RowExplanation struct {
	CurrentLSN  int64
	SnapshotLSN int64
	HistoryRows []map[string]any
}

func ExplainRowAtCurrentHead(ctx context.Context, conn *pgx.Conn, table string, pk any) (*RowExplanation, error) {
	var currentLSN int64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&currentLSN); err != nil {
		return nil, err
	}

	historyRows, err := QueryHistory(ctx, conn, table, pk)
	if err != nil {
		return nil, err
	}
	defer historyRows.Close()

	result := &RowExplanation{CurrentLSN: currentLSN, SnapshotLSN: currentLSN}
	for historyRows.Next() {
		values, err := historyRows.Values()
		if err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, fd := range historyRows.FieldDescriptions() {
			row[string(fd.Name)] = values[i]
		}
		result.HistoryRows = append(result.HistoryRows, row)
	}
	if err := historyRows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
```

Use this pattern when the service wants one explicit place where:

- the current head is captured,
- history is collected,
- the `LSN` used for explanation is explicit,
- and the caller can still issue one `AS OF LSN` query with that token.

For aggregate-oriented services, swap the snapshot token source to:

- `entity_version(...)` when the caller wants the current business version,
- then `entity_version_lsn(...)` when the actual snapshot read needs `AS OF LSN`.

---

## Programmatic usage reference

See also:

- [docs/getting-started/09-go-sdk-and-integration.md](../getting-started/09-go-sdk-and-integration.md)
- [`sdk/`](../../sdk/) — typed Go client on top of the gRPC admin surface.

pgwire should be the default integration path for new services; the gRPC admin
surface is reserved for lower-level engine-oriented operations.