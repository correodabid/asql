# Transaction and Protocol Spec (MVP)

## SQL transaction primitives
### Single-domain
```sql
BEGIN DOMAIN accounts;
-- DML/DDL subset
COMMIT;
```

### Cross-domain
```sql
BEGIN CROSS DOMAIN accounts, loans;
-- statements over declared domains only
COMMIT;
```

## Rules
1. A transaction MUST declare domain scope at `BEGIN`.
2. Statements touching undeclared domains MUST fail.
3. Cross-domain transactions preserve client statement order within the declared domain scope.
4. Commit is atomic from client perspective.
5. Any conflict aborts transaction with deterministic error code.

## WAL record schema (conceptual)
```text
Record {
  lsn: uint64
  tx_id: string
  ts_logical: uint64
  type: BEGIN | MUTATION | COMMIT | ABORT | SNAPSHOT
  domains: []string
  payload: bytes
  checksum: uint32
  version: uint16
}
```

## Deterministic guarantees
- `lsn` is strictly monotonic per node.
- Domain list in records is canonicalized (sorted).
- Mutation payload uses stable serialization.
- Replay validates checksum/version before apply.

## Runtime surface notes

Canonical application path:
- PostgreSQL-compatible subset over pgwire,
- explicit transaction scope using `BEGIN DOMAIN ...` and `BEGIN CROSS DOMAIN ...`,
- historical reads via SQL syntax and helper functions.

Secondary/admin surface:
- internal or optional gRPC paths may still expose `Execute`, `BeginTx`, `CommitTx`, `RollbackTx`, `ReplayToLSN`, and `TimeTravelQuery`,
- these should not be treated as the primary adoption or compatibility surface.

## Error model
Use stable machine-readable error codes:
- `ERR_DOMAIN_UNDECLARED`
- `ERR_TX_CONFLICT`
- `ERR_TX_NOT_FOUND`
- `ERR_REPLAY_CORRUPTED_LOG`
- `ERR_UNSUPPORTED_SQL`

## Compatibility stance

ASQL exposes a pragmatic PostgreSQL-compatible subset over pgwire, but it is
not a drop-in PostgreSQL replacement. See
[docs/reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
and [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md).

For current implementation priorities, use [docs/ai/05-backlog.md](05-backlog.md) as the source of truth rather than this MVP-era protocol note.

## Time-travel semantics
- `AS OF LSN <n>`: read from materialized state as of log position.
- `AS OF TIMESTAMP <t>`: map to nearest <= logical timestamp.
- Time-travel queries are read-only.

## Replication (optional v1)
- Leader per domain group.
- Replicate committed WAL records.
- Follower applies records in `lsn` order only.
- No conflict resolution in follower; reject out-of-order records.
