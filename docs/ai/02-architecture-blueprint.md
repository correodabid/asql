# ASQL Architecture Blueprint

## High-level architecture
```text
Clients
  | pgwire / gRPC
  v
/internal/server/pgwire  ---> /internal/server/grpc (cluster sidecar, optional)
            |                           |
            v                           v
/internal/engine
  parser -> planner -> executor -> tx coordinator -> storage adapters
                                   |
                                   v
                              /internal/storage
                                wal + btree
```

## Hexagonal boundaries
### Core (pure)
- SQL AST and planning.
- Transaction orchestration (single-domain and cross-domain).
- Deterministic state transitions.
- Replay engine.

### Ports
- `LogStore` (append/read log entries).
- `KVStore` or page storage.
- `DomainCatalog`.
- `Clock` and `Entropy` (determinism-safe).
- `Telemetry` hooks.

### Adapters
- WAL file adapter.
- B-Tree storage adapter.
- pgwire transport adapter.
- gRPC transport adapter.
- Optional replication adapter.

Production runtime note:
- clustered production runtime is `internal/server/pgwire` + Raft,
- standalone `internal/server/grpc` may still host transitional heartbeat-led cluster behavior, but should not be treated as the canonical production cluster path.

## Domain model
Each domain contains:
- isolated schema namespace,
- independent physical storage partition,
- independent domain metadata and constraints,
- explicit participation in cross-domain tx.

## Deterministic execution model
- All writes become ordered log entries with monotonic `LSN`.
- Materialized state is derived from WAL + snapshots.
- Replay applies log entries in canonical order only.
- Concurrent scheduling must not alter observable results.

## Suggested internal packages
```text
/internal/engine/parser
/internal/engine/planner
/internal/engine/executor
/internal/engine/domains
/internal/engine/tx
/internal/engine/mvcc
/internal/engine/replay
/internal/storage/wal
/internal/storage/btree
/internal/server/grpc
/internal/cluster/coordinator
/internal/cluster/replication
/internal/platform/clock
/internal/platform/logging
/internal/platform/telemetry
```

## Data lifecycle
1. Parse SQL request.
2. Bind to domain context.
3. Plan deterministic execution path.
4. Build write set/read set.
5. Append commit intent to WAL.
6. Apply to storage.
7. Emit commit record.
8. Publish telemetry event.

## Observability baseline
- Structured logs with `request_id`, `tx_id`, `domain`, `lsn`.
- Metrics:
  - tx commit latency,
  - replay throughput,
  - WAL append duration,
  - conflict/abort counts.
- Traces:
  - parse/planner/executor spans,
  - WAL append span,
  - replication span (when enabled).
