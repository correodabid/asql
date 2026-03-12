# ASQL Go Cookbook

This cookbook provides practical, runnable Go-centric workflows using ASQL APIs and examples.

## Prerequisites

- ASQL server running on `127.0.0.1:9042`
- Go `1.24.x`

Start server:

```bash
go run ./cmd/asqld -addr :9042 -data-dir .asql
```

---

## Recipe 1: Schema init + first write

Initialize schema once and run first transaction:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -init-schema -id 1 -email bootstrap@example.com
```

Expected:
- schema initialization succeeds,
- transaction commit succeeds,
- rollback/cross-domain demo path completes.

---

## Recipe 2: Cross-domain transaction lifecycle

Run a normal write while exercising cross-domain begin + rollback flow:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -secondary-domain app_aux -table users -id 2 -email second@example.com
```

Expected:
- domain transaction committed,
- rollback demo completed,
- cross-domain begin/rollback completed.

---

## Recipe 3: Admin-check (time-travel + replay)

Run full flow with admin verification:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -secondary-domain app_aux -table users -id 3 -email admin@example.com -verify-admin
```

Optional strict mode:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -secondary-domain app_aux -table users -id 4 -email strict@example.com -verify-admin -verify-admin-strict
```

Expected in non-strict mode:
- admin errors are reported as warnings,
- write flow remains successful.

---

## Programmatic usage reference

See the example client implementation:

- `examples/go-client/main.go`

Core operations demonstrated there:
- BeginTx (`domain` and `cross`)
- Execute
- CommitTx / RollbackTx
- TimeTravelQuery (LSN and logical timestamp)
- ReplayToLSN