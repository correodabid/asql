# 02. Install and Run

## Prerequisites

Minimum local setup:

- Go `1.24.x`
- a shell with `make` available
- optional: Docker

## Validate the repository once

```bash
go test ./...
```

This confirms the local toolchain is compatible with the repository.

## Start ASQL locally

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

What this does:

- starts the local engine,
- persists WAL and snapshots in `.asql`,
- exposes the pgwire endpoint on `127.0.0.1:5433`.

Keep this process running while you work.

## Optional interactive shell

In a second terminal you can open the built-in shell:

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## Start the desktop Studio

In a second terminal:

```bash
go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

Use Studio when you want:

- schema browsing,
- row inspection,
- time explorer workflows,
- fixture validate/load/export workflows.

## Optional smoke path

If you want a single validation command:

```bash
make smoke-onboarding
```

## Optional Docker path

```bash
docker build -t asql:local .
docker run -p 5433:5433 -v $(pwd)/.data:/data asql:local
```

## What to do next

After the server is running, continue with [03-first-database.md](03-first-database.md).

## Common startup issues

### `connection refused`

The server process is not running or is listening on a different port.

### stale local data directory

If you are doing repeated experiments and want a clean start:

```bash
rm -rf .asql
```

Only do this for local disposable data.
