# ASQL Technical Runbook

This runbook documents the current end-to-end execution path in the repository.

Related quick docs:
- `docs/getting-started-10-min.md`
- `docs/cookbook-go-sdk.md`
- `docs/migration-sqlite-quick-path.md`

Backup and point-in-time recovery validation are covered by integration tests:
- `test/integration/backup_restore_test.go`
- `test/integration/recovery_restore_test.go`

## 1) Prerequisites

- Go `1.24.x`
- macOS/Linux shell

## 2) Validate full system

Run the full suite:

```bash
go test ./...
```

This validates:

- parser/planner deterministic snapshots,
- single-domain and cross-domain transactions,
- WAL append/recover/checksum,
- replay and time-travel APIs,
- gRPC black-box behavior,
- replication catch-up and divergence checks.

## 3) Run ASQL server manually

```bash
go run ./cmd/asqld -addr :9042 -data-dir .asql
go run ./cmd/asqld -addr :9042 -admin-addr :9090 -data-dir .asql

# optional shared pgwire password / bearer token auth
go run ./cmd/asqld -addr :9042 -data-dir .asql -auth-token my-secret

# optional mTLS authn
go run ./cmd/asqld -addr :9042 -data-dir .asql \
	-tls-cert ./certs/server.pem \
	-tls-key ./certs/server-key.pem \
	-tls-client-ca ./certs/ca.pem
```

Runtime flags:

- `-addr`: gRPC bind address
- `-admin-addr`: optional admin HTTP bind address for `/metrics`, `/readyz`, `/livez`, `/api/v1/health`, `/api/v1/leadership-state`, `/api/v1/last-lsn`, `/api/v1/failover-history`, `/api/v1/snapshot-catalog`, and `/api/v1/wal-retention`
- `-data-dir`: data directory path (default `.asql`)
- `-auth-token`: optional shared secret used as the pgwire password and as the Bearer token for gRPC/admin APIs
- `-tls-cert`: server certificate path for mTLS
- `-tls-key`: server private key path for mTLS
- `-tls-client-ca`: CA certificate path used to verify client certificates

If `-auth-token` is configured, clients must send:

- `authorization: Bearer <token>`

Pgwire clients must connect with the same token as the password, for example:

- `postgres://asql:<token>@host:5433/asql?sslmode=disable`

If any TLS flag is configured, all three TLS flags are required and client certificates
must chain to `-tls-client-ca`.

## 3.1) Admin HTTP metrics and health

When `-admin-addr` is enabled, the pgwire runtime exposes first-class operator endpoints:

- `/metrics`: Prometheus text metrics for readiness, WAL durability, commit/fsync latency, replay and snapshot durations, audit errors, throughput, file sizes, current leader state, replication lag, and failover totals.
- `/livez`: liveness probe for process supervision.
- `/readyz`: readiness probe that fails closed while clustered runtime is up but no Raft leader is currently known.
- `/api/v1/health`: JSON health summary.
- `/api/v1/leadership-state`: JSON leadership state including `leader_id`, `term`, `lease_active`, and `last_safe_lsn`.
- `/api/v1/last-lsn`: JSON last durable WAL LSN.
- `/api/v1/failover-history`: most recent serialized failover transitions observed by the local node.
- `/api/v1/snapshot-catalog`: persisted snapshot checkpoint catalog for the local data dir.
- `/api/v1/wal-retention`: retained WAL window, segment metadata, and snapshot retention state.

Example:

```bash
go run ./cmd/asqld -addr :9042 -admin-addr :9090 -data-dir .asql
curl -s http://127.0.0.1:9090/readyz
curl -s http://127.0.0.1:9090/api/v1/leadership-state
curl -s http://127.0.0.1:9090/api/v1/failover-history
curl -s http://127.0.0.1:9090/api/v1/wal-retention
curl -s http://127.0.0.1:9090/metrics | grep 'asql_cluster_'
```

## 3.2) Audit events

gRPC transaction and admin APIs emit structured audit logs with message `audit_event` and fields:

- `event=audit`
- `status=success|failure`
- `operation` (for example `tx.begin`, `tx.execute`, `tx.commit`, `admin.time_travel_query`, `admin.replay_to_lsn`, `admin.replication_stream`)
- contextual fields such as `tx_id`, `lsn`, `domains`, and `reason` for failures

## 4) Single-node transaction flow (verified)

Use the black-box test as executable documentation:

```bash
go test ./internal/server/grpc -run TestASQLServiceBlackBox -v
```

It covers:

- `BeginTx` (domain mode)
- `Execute` for `CREATE TABLE` and `INSERT`
- `CommitTx`
- `TimeTravelQuery`
- `ReplayToLSN`

## 5) Replay + time-travel flow (verified)

```bash
go test ./test/integration -run TestReplayToLSNAndTimeTravelQueries -v
```

It validates:

- historical reads `as-of LSN`,
- timestamp-to-LSN mapping,
- state rewind via `ReplayToLSN`.

## 6) Replication flow leader -> follower (verified)

```bash
go test ./internal/cluster/replication -run TestLeaderFollowerCatchUp -v
```

It validates:

- replication stream RPC (`StreamWAL`),
- follower strict LSN apply,
- follower state catch-up after replay.

## 7) Divergence detection behavior

```bash
go test ./internal/cluster/replication -run TestApplyBatchDetectsDivergence -v
```

This test proves out-of-order records are rejected deterministically.

## 8) Useful dev commands

```bash
go test ./internal/engine/executor -v
go test ./internal/storage/wal -v
go test ./internal/server/grpc -v
go test ./test/integration -run TestBackupWipeRestorePreservesQueryParity -v
go test ./test/integration -run TestBaseBackupRestoreToLSNAndTimestamp -v
make security-scan
```

## 8.1) Base backup + restore-to-LSN / restore-to-timestamp

The repository now includes recovery primitives in the executor layer for exact restore boundaries:

- `CreateBaseBackup(sourceDataDir, backupDir)`
- `RestoreBaseBackupToLSN(ctx, backupDir, targetDataDir, targetLSN)`
- `RestoreBaseBackupToTimestamp(ctx, backupDir, targetDataDir, logicalTimestamp)`

The base backup manifest is written to `meta/base-backup.json` and records:

- snapshot files included in the backup,
- WAL segment catalog entries,
- head LSN and head logical timestamp,
- persisted timestamp-index artifact when present.

Operator CLI commands:

```bash
go run ./cmd/asqlctl -command backup-create -data-dir .asql -backup-dir .asql-backup
go run ./cmd/asqlctl -command backup-manifest -backup-dir .asql-backup
go run ./cmd/asqlctl -command backup-verify -backup-dir .asql-backup
go run ./cmd/asqlctl -command restore-lsn -backup-dir .asql-backup -data-dir .asql-restore-lsn -lsn 123
go run ./cmd/asqlctl -command restore-timestamp -backup-dir .asql-backup -data-dir .asql-restore-ts -logical-ts 123
go run ./cmd/asqlctl -command snapshot-catalog -data-dir .asql
go run ./cmd/asqlctl -command wal-retention -data-dir .asql
```

Operational notes:

- `backup-verify` validates every manifest-referenced snapshot, WAL segment, and timestamp-index artifact against recorded SHA-256 checksums.
- restore commands fail closed when backup artifacts are corrupted or when persisted snapshots and WAL history are inconsistent.
- `restore-lsn` and `restore-timestamp` create a recoverable target data directory that can be reopened directly with `asqld` or the engine APIs.

Executable recovery reference:

```bash
go test ./test/integration -run TestBaseBackupRestoreToLSNAndTimestamp -v
```

That test proves:

- a base backup can be created from a populated data directory,
- restore can stop at an exact LSN boundary,
- restore can stop at the latest LSN at-or-before a logical timestamp,
- reopened engines preserve the recovered state after restart.

Failure-path validation references:

```bash
go test ./internal/storage/wal -run TestSegmentedLogStoreRecoverAfterInjectedPartialTail -v
go test ./internal/engine/executor -run 'TestReplayFailsClosedOnCorruptSnapshotFile|TestReplayFailsOnSnapshotWALGap' -v
go test ./test/integration -run TestBaseBackupVerificationFailsOnChecksumMismatch -v
```

Those checks prove:

- torn-write tails are truncated back to the last valid WAL frame,
- corrupted snapshots stop restart instead of being silently ignored,
- snapshot/WAL gaps stop replay instead of producing a partial state,
- backup checksum drift is detected before restore mutates the target data directory.

`make security-scan` runs:

- `go mod verify` (dependency integrity)
- `govulncheck ./...` (known Go vulnerability scanning)

`make sbom` runs:

- `syft dir:. -o spdx-json > sbom.spdx.json`

CI security pipeline also generates and uploads `sbom.spdx.json` on each run.
On `v*` tags, CI builds release binaries, signs `checksums.txt` with Sigstore keyless
(`cosign sign-blob`), and publishes release assets.

## 9) Manual DB workflow with asqlctl

Start server with your own WAL file:

```bash
go run ./cmd/asqld -addr :9042 -data-dir .asql
```

Begin transaction:

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command begin -mode domain -domains app
```

Execute statements with returned `tx_id`:

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command execute -tx-id <tx_id> -sql "CREATE TABLE users (id INT, email TEXT)"
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command execute -tx-id <tx_id> -sql "INSERT INTO users (id, email) VALUES (1, 'you@example.com')"
```

Commit or rollback:

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command commit -tx-id <tx_id>
# or
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command rollback -tx-id <tx_id>
```

Time-travel and replay:

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command time-travel -domains app -lsn 4 -sql "SELECT id, email FROM users"
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 -command replay -lsn 4
```

If server auth is enabled, add `-auth-token <token>` to all `asqlctl` commands.
