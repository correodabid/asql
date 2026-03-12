# ASQL – Copilot Instructions

## Project intent
Build **ASQL**, a deterministic SQL engine in Go with:
- single-node embedded core first,
- domain isolation inside the engine,
- deterministic append-only log and replay,
- optional distributed execution/replication via gRPC.

The product target is practical and commercial: edge/offline-first apps, microservice backends, and compliance/audit-heavy systems.

## Product principles (non-negotiable)
1. **Single-node first**: every feature must work locally before distributed mode.
2. **Determinism first**: same input log => same state and query results.
3. **Domain isolation**: each domain has isolated schema, storage, and rules.
4. **Cross-domain by protocol**: explicit transaction coordinator; never implicit side effects.
5. **Append-only truth**: WAL/event log is the source of truth; materialized state is derived.
6. **Observability by default**: structured logs, metrics, traces, and replay tooling.
7. **Minimal surface area**: keep the engine opinionated and compact; avoid premature extensibility.

## Scope policy
### In scope (MVP)
- SQL subset (DDL + DML basics + transactions).
- JSON columns/operators (minimum useful subset).
- Domain-aware transactions:
  - `BEGIN DOMAIN <name>`
  - `BEGIN CROSS DOMAIN <a>, <b>`
- Deterministic WAL and replay.
- Time-travel reads based on log position/timestamp.
- gRPC API for client operations.
- Optional replication mode (single leader per domain group in v1).

### Out of scope (MVP)
- Full ANSI SQL compatibility.
- Cost-based optimizer.
- Arbitrary distributed sharding.
- Multi-region consensus complexity.
- Smart-contract-like runtime.

## Architecture constraints
- Language: Go.
- Internal architecture style: hexagonal/ports-and-adapters.
- Keep pure core modules free from transport/storage framework coupling.
- No hidden global mutable state in execution path.
- Deterministic clocks/randomness abstraction in core (`Clock`, `Entropy` interfaces).

## Target repository shape
Prefer this structure as implementation starts:

```text
/cmd
  /asqld
  /asqlctl
/internal
  /engine
    /parser
    /planner
    /executor
    /domains
    /tx
    /mvcc
    /replay
  /storage
    /wal
    /btree
  /cluster
    /coordinator
    /replication
  /server
    /grpc
  /platform
    /clock
    /logging
    /telemetry
/api
  /proto
/test
  /integration
  /determinism
/docs
```

## Engineering standards
- Go formatting/linting/tests are mandatory on every PR.
- Public interfaces must include doc comments.
- Every state transition must be testable deterministically.
- Add at least one integration test for every new engine capability.
- Preserve backward compatibility of log format once declared stable.

## Determinism checklist (apply on every PR)
- Does this change depend on wall-clock time directly?
- Does this change depend on map iteration order?
- Does this change use randomness without an injected seed/source?
- Does this change introduce non-deterministic concurrency ordering?
- Can replay produce identical snapshots and query outputs?

If any answer is “yes” (except the last), refactor before merging.

## Agent workflow
1. Read `docs/ai/01-product-vision.md`.
2. Pick next task from `docs/ai/05-backlog.md`.
3. Implement smallest vertical slice.
4. Add/adjust tests.
5. Update docs impacted by the change.
6. Record ADR when architecture-significant.

## Definition of done (task level)
A task is done only if:
- code compiles,
- tests pass,
- determinism constraints verified,
- observability hooks added where relevant,
- documentation updated.

## Communication style for AI agents
- Be concise and explicit about assumptions.
- Propose incremental changes over large rewrites.
- Call out trade-offs and unresolved risks.
- If ambiguous, choose the simplest interpretation that preserves determinism.
