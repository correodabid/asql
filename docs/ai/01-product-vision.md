# ASQL Product Vision

## One-line definition
ASQL is a deterministic SQL engine for Go applications that combines embedded simplicity with optional distributed domain scaling.

## Problem statement
Current choices force trade-offs:
- embedded DBs are simple but weak in replication/domain isolation,
- distributed DBs are powerful but operationally heavy,
- microservices solve modularity at the cost of consistency and debugging complexity.

ASQL aims to deliver:
- local-first developer experience,
- deterministic replay for debugging/audit,
- domain-isolated data boundaries,
- optional remote domain execution.

## Ideal users
- backend teams building event-driven systems,
- edge/offline-first platforms,
- fintech/healthcare/compliance-heavy products,
- teams that need reproducibility and temporal queries.

## Core differentiators
1. Deterministic replayable state.
2. Domain-isolated SQL model.
3. Cross-domain transaction coordination.
4. Native gRPC API.
5. Time-travel queries over append-only history.

## MVP success criteria
- Run in-process with no external dependency.
- Persist and replay deterministic WAL.
- Support basic domain and cross-domain transactions.
- Expose gRPC endpoint for CRUD + tx + replay controls.
- Demonstrate time-travel read in integration tests.

## Non-goals for MVP
- Full distributed SQL feature parity.
- Full SQL standard compliance.
- Multi-tenant cloud control plane.
