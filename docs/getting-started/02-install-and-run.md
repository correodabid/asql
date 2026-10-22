# 02. Install and Run

## Prerequisites

Minimum local setup:

- Go `1.26` or newer (or a release binary, below)
- a shell with `make` available
- optional: Docker

## Validate the repository once

```bash
go test ./...
```

This confirms the local toolchain is compatible with the repository.

## Start ASQL locally

:::tabs
:::tab[Go run]
```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

Starts the engine, persists WAL and snapshots in `.asql`, and exposes the pgwire endpoint on `127.0.0.1:5433`. Keep this process running while you work.
:::tab[Release binary]
```bash
gh release download -R correodabid/asql      # the latest release
cosign verify-blob --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github.com/correodabid/asql/\.github/workflows/ci\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com checksums.txt
sha256sum -c checksums.txt
chmod +x asqld-linux-amd64 && ./asqld-linux-amd64 -addr :5433 -data-dir .asql
```

Releases ship `asqld` and `asqlctl` for linux/amd64 and darwin/arm64, with checksums signed by the release workflow. Read the [changelog](../../CHANGELOG.md)'s upgrade notes before pointing a new version at an existing data directory.
:::tab[Docker]
```bash
docker build -t asql:local .
docker run -p 5433:5433 -v $(pwd)/.data:/data asql:local
```

This container starts `asqld` with its default entrypoint and persists engine state under `/data/.asql` inside the mounted volume.
:::

## Optional interactive shell

In a second terminal you can open the built-in shell:

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## Start the desktop Studio

ASQL Studio lives in a separate repository:
[github.com/correodabid/asqlstudio](https://github.com/correodabid/asqlstudio).
Clone it next to your asql checkout, then in a second terminal:

```bash
cd ../asqlstudio
wails dev
```

Studio connects to the asqld you already have running on `127.0.0.1:5433`.

Use Studio when you want:

- schema browsing,
- row inspection,
- time explorer workflows,
- fixture validate/load/export workflows.

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

:::danger[Destructive — cannot be undone]
This permanently deletes all WAL records and snapshots. Only run it on local development data.
:::
