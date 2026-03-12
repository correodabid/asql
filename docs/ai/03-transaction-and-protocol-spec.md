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
3. Cross-domain transactions use deterministic coordinator ordering (lexicographic domain order).
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

## gRPC surface (MVP)
### Service: `ASQLService`
- `Execute(ExecuteRequest) returns (ExecuteResponse)`
- `BeginTx(BeginTxRequest) returns (BeginTxResponse)`
- `CommitTx(CommitTxRequest) returns (CommitTxResponse)`
- `RollbackTx(RollbackTxRequest) returns (RollbackTxResponse)`
- `ReplayToLSN(ReplayToLSNRequest) returns (ReplayToLSNResponse)`
- `TimeTravelQuery(TimeTravelQueryRequest) returns (TimeTravelQueryResponse)`

## Error model
Use stable machine-readable error codes:
- `ERR_DOMAIN_UNDECLARED`
- `ERR_TX_CONFLICT`
- `ERR_TX_NOT_FOUND`
- `ERR_REPLAY_CORRUPTED_LOG`
- `ERR_UNSUPPORTED_SQL`

## Time-travel semantics
- `AS OF LSN <n>`: read from materialized state as of log position.
- `AS OF TIMESTAMP <t>`: map to nearest <= logical timestamp.
- Time-travel queries are read-only.

## Replication (optional v1)
- Leader per domain group.
- Replicate committed WAL records.
- Follower applies records in `lsn` order only.
- No conflict resolution in follower; reject out-of-order records.
