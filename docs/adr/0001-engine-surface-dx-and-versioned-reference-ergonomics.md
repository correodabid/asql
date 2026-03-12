# ADR 0001: Improve ASQL developer surface for temporal references and history

- Status: Proposed
- Date: 2026-03-12
- Decision drivers:
  - preserve ASQL's deterministic and temporal core
  - reduce developer exposure to raw storage mechanics
  - make realistic multi-domain applications and fixtures practical
  - clarify product expectations around SQL and pgwire behavior

Implementation update (2026-03-12): substantial portions of this direction have since been implemented or documented, including compatibility guidance, fixture workflow improvements, temporal helper surfacing, and versioned-reference ergonomics. Treat this ADR as the durable rationale; use [docs/ai/05-backlog.md](../ai/05-backlog.md) and the reference docs for current implementation state.

## Context

While building and seeding Hospital MiKS on top of ASQL, several recurring frictions appeared:

1. versioned foreign keys are conceptually correct but operationally awkward in normal application workflows;
2. raw `LSN` handling leaks into application and seed code;
3. the engine lacks ergonomic introspection helpers for row/entity temporal state;
4. `FOR HISTORY` is valuable but not yet strong enough as a stable public contract;
5. PostgreSQL compatibility expectations are not explicit enough for the current product surface;
6. deterministic fixtures and demo seeds are harder than they should be;
7. application-facing modeling is still too close to rows and commit positions instead of aggregates and business episodes.

These issues do not invalidate ASQL's direction. They indicate that the public surface is still too close to internal engine mechanics.

## Decision

ASQL will keep its deterministic, append-only, time-travel-oriented core, but will shift the application-facing surface toward safer and higher-level temporal workflows.

Specifically:

1. Versioned foreign keys remain part of the product.
2. Common write paths must no longer require explicit raw `LSN` management.
3. The engine should automatically resolve visible reference versions for versioned foreign keys during normal `INSERT` and `UPDATE` flows.
4. ASQL should provide first-class introspection helpers for current row, entity, and snapshot metadata.
5. `FOR HISTORY` will be treated as a stable public contract with documented shape, naming, and types.
6. The product must publish a clear SQL and pgwire compatibility stance.
7. Deterministic fixtures and seed workflows will become first-class capabilities.
8. Application-facing evolution should bias toward entity and aggregate semantics rather than row-plus-`LSN` mechanics.

## Decision details

### 1. Automatic versioned-reference capture

For normal writes, the engine should resolve referenced visible versions automatically from the transaction snapshot.

Target direction:

```sql
INSERT INTO invoices (patient_id, admission_id, amount_cents)
VALUES ('patient-1', 'admission-1', 125000);
```

The engine should derive any required temporal reference metadata internally unless the caller explicitly opts into advanced control.

### 2. Explicit advanced override remains available

Advanced users may still need precise historical control. ASQL should keep an explicit override mode for reference-version selection, but that mode should be opt-in rather than mandatory.

### 3. Supported temporal introspection primitives

ASQL should expose stable helpers for tooling and diagnostics, such as:

- `current_lsn()`
- `row_lsn(domain.table, id)`
- `entity_version(domain, entity, id)`
- `entity_head_lsn(domain, entity, id)`
- `resolve_reference(...)`
- `resolve_reference_as_of(...)`

Exact names may evolve during implementation, but the capability set is part of the decision.

### 4. Stable history contract

`FOR HISTORY` must become contract-grade. At minimum, ASQL must define and preserve:

- metadata column names
- metadata column types
- nullability rules
- row-image semantics
- client-facing expectations across pgwire, CLI, and SDKs

### 5. Compatibility policy

ASQL must choose and document one clear stance:

- a pragmatic PostgreSQL-compatible subset with guarantees for common workflows, or
- an explicitly ASQL-native dialect/tooling profile.

Ambiguity is not acceptable.

### 6. Fixtures as a product capability

ASQL should provide deterministic fixture import/export and validation workflows suitable for demos, tests, benchmarks, and design-partner environments.

### 7. Aggregate-first evolution

ASQL should increasingly expose concepts aligned with business aggregates and lifecycle snapshots, especially for regulated workloads such as healthcare, manufacturing, and finance.

## Consequences

### Positive

- simpler application and seed code
- lower cognitive load for developers
- stronger product credibility for temporal and audit use cases
- fewer ad hoc workarounds outside the engine
- better support for demos, tests, and reproducible benchmarks
- better alignment between engine semantics and business workflows

### Negative / costs

- additional engine and contract design work
- need to freeze and support public metadata shapes
- risk of short-term implementation complexity in planner, executor, and history APIs
- likely documentation and compatibility-matrix maintenance burden

### Neutral / preserved

- determinism remains a core product principle
- raw `LSN` access remains available for advanced workflows
- domain isolation and replay-first architecture remain unchanged

## Alternatives considered

### Alternative A: Keep current model and improve docs only

Rejected.

Documentation alone does not solve the core problem that normal workflows currently require engine-level reasoning.

### Alternative B: Remove or weaken versioned foreign keys

Rejected.

Versioned references are aligned with ASQL's temporal model and regulated-workload value proposition. The issue is ergonomics, not capability existence.

### Alternative C: Fully hide temporal internals everywhere

Rejected.

Advanced users and operational tooling still need access to raw commit-position and snapshot mechanics. The goal is not removal, but layering.

## Implementation guidance

Execution should proceed in four steps:

1. contract stabilization
   - stabilize `FOR HISTORY`
   - publish compatibility boundaries
2. temporal introspection
   - add row/entity/snapshot helper functions
3. reference ergonomics
   - implement automatic versioned-reference capture and explicit override mode
4. fixture/product workflow
   - add deterministic fixture tooling and aggregate-oriented documentation

## Acceptance signals

This ADR is successful when:

- realistic multi-domain seeds no longer need manual `LSN` plumbing in ordinary cases;
- `FOR HISTORY` can be consumed reliably by CLI, pgwire clients, and SDKs;
- ASQL's compatibility expectations are explicit and easy to understand;
- temporal tooling uses stable helper APIs rather than reverse-engineered behavior;
- application authors can think in terms of business entities and snapshots more often than raw commit positions.

## Related documents

- [docs/ai/05-backlog.md](../ai/05-backlog.md)
