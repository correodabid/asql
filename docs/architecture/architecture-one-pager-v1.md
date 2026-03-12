# ASQL Architecture One-Pager (v1)

Date: 2026-03-01
Audience: solution architects, platform engineering, and technical decision-makers.

## 1) One-line architecture summary

ASQL is a deterministic SQL engine in Go that runs embedded first, with domain-isolated data boundaries, append-only WAL as source of truth, and optional distributed replication driven by the pgwire + Raft runtime.

## 2) High-level layout

```text
Clients
  | pgwire (application path) / gRPC (optional cluster-admin path)
  v
/cmd/asqld
  |\\
  | \\--> /internal/server/grpc (optional)
  v
/internal/server/pgwire
      |
      v
/internal/engine
  parser -> planner -> executor -> tx coordinator -> storage adapters
                                   |
                                   v
                              /internal/storage
                                wal + btree
```

Production note:
- canonical local and application-facing runtime: `cmd/asqld` with pgwire enabled,
- clustered runtime layers pgwire + Raft, with gRPC retained for cluster/admin transport where enabled,
- standalone gRPC usage is secondary and should not be treated as the default onboarding path.

## 3) Core architectural principles

- **Single-node first**: every capability works locally before distributed mode.
- **Determinism first**: same WAL input sequence yields same state and query results.
- **Domain isolation**: schemas and constraints are isolated per domain.
- **Append-only truth**: WAL is canonical; materialized state is derived.
- **Observability by default**: logs, metrics, and replay-oriented diagnostics are built-in.

## 4) Execution and transaction model

- Deterministic execution pipeline: parse -> plan -> execute -> WAL commit records.
- Single-domain and cross-domain transactions are explicit at `BEGIN`.
- Cross-domain commit ordering is canonicalized to preserve deterministic behavior.
- Time-travel reads support `AS OF LSN` and logical timestamp mapping.

Aggregate-facing note:
- [docs/reference/aggregate-reference-semantics-v1.md](../reference/aggregate-reference-semantics-v1.md) describes how entity versions and aggregate references sit on top of the deterministic row-and-WAL core.

## 5) Hexagonal boundaries

### Pure core
- parser/planner/executor, transaction orchestration, replay logic.

### Ports
- `LogStore`, `KVStore`, `DomainCatalog`, `Clock`, `Entropy`, `Telemetry`.

### Adapters
- file WAL adapter, btree storage adapter, pgwire transport, gRPC transport, optional replication adapter.

This keeps execution core free from transport/storage framework coupling.

## 6) Deterministic data lifecycle

1. Parse SQL request.
2. Bind domain context.
3. Build deterministic execution path.
4. Emit ordered WAL records with monotonic `LSN`.
5. Apply materialized state updates.
6. Replay in canonical order for recovery/history.

## 7) Why this matters for production teams

- Faster root-cause analysis with replayable history.
- Safer multi-domain boundaries via explicit transaction scope.
- Predictable behavior under restart/recovery.
- Practical path from embedded/local usage to replicated deployments.

## 8) Reference specs

- `docs/ai/02-architecture-blueprint.md`
- `docs/ai/03-transaction-and-protocol-spec.md`
- `docs/ai/01-product-vision.md`
