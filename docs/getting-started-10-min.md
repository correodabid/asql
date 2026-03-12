# ASQL Getting Started (10 Minutes)

This guide gets you from zero to a successful transaction and replay validation quickly.

## Prerequisites

- Go `1.24.x`
- `python3`
- Optional: Docker

## 1) Validate project health

```bash
go test ./...
```

## 2) Start ASQL locally

```bash
go run ./cmd/asqld -addr :9042 -data-dir .asql
```

Keep this terminal open.

## 3) Initialize schema once

In a second terminal:

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -secondary-domain app_aux -table users -id 1 -email bootstrap@example.com -init-schema
```

## 4) Run a full feature flow

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -secondary-domain app_aux -table users -id 2 -email second@example.com -verify-admin
```

This executes:
- domain transaction (insert + commit),
- rollback demo,
- cross-domain begin/rollback,
- time-travel by LSN,
- time-travel by timestamp,
- replay to latest LSN.

## 5) One-command onboarding smoke test

```bash
make smoke-onboarding
```

It starts a temporary ASQL instance on a random local port and validates the full onboarding path.

## 6) Optional Docker path

```bash
make docker-build
make docker-run
```

Then run client commands against `127.0.0.1:9042`.

## Troubleshooting

- `connection refused`: ensure `asqld` is running.
- `table not found`: run once with `-init-schema`.
- admin replay warning: run on a fresh WAL or keep admin checks in best-effort mode.