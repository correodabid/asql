# ASQL Engine DX Actionable Backlog v1

> Historical note
>
> This backlog captured an earlier DX stabilization phase and is no longer the current execution source of truth.
>
> Use these docs instead for current direction:
>
> - [docs/getting-started/README.md](getting-started/README.md) for onboarding guidance
> - [docs/asql-adoption-friction-prioritized-backlog-v1.md](asql-adoption-friction-prioritized-backlog-v1.md) for current adoption work
> - [docs/ai/05-backlog.md](ai/05-backlog.md) for active engineering execution
> - [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](adr/0002-generalist-engine-boundary-and-adoption-surface.md) for the current product boundary decision

## Purpose

Turn the findings in the engine DX and versioned-reference improvement plan into an execution backlog that can be implemented incrementally without weakening ASQL's deterministic core.

## Outcome target

A developer building a realistic multi-domain application should be able to:

- write normal inserts without manual `LSN` bookkeeping,
- inspect temporal metadata through stable supported functions,
- rely on `FOR HISTORY` as a documented contract,
- understand exactly which SQL and pgwire behaviors are supported,
- load deterministic fixtures for demos, tests, and benchmarks.

## Prioritization model

- `P0`: blocks normal app development or damages trust in core product surface
- `P1`: high leverage for design partners, demos, SDKs, and integration quality
- `P2`: important productization and workflow acceleration
- `P3`: strategic follow-on improvements after the DX baseline is stable

## Execution rules

- preserve determinism in every task
- prefer smallest vertical slice that produces a usable contract
- add integration tests for every public-facing temporal feature
- update docs and examples in the same change when behavior becomes public
- do not expose unstable engine internals as accidental public contracts

## Epic W — History contract stabilization (`P0`)

### Goal

Make `FOR HISTORY` safe to use from CLI, pgwire clients, SDKs, and tests without reverse engineering.

### Tasks

- [x] Define canonical `FOR HISTORY` output contract.
  - Decide metadata columns, names, order, and types.
  - Define nullability and row-image semantics.
  - Definition of done:
    - spec documented
    - contract tests added
    - examples added for CLI and Go usage

- [x] Make pgwire `FOR HISTORY` output match the documented contract.
  - Ensure consistent metadata across supported clients.
  - Definition of done:
    - integration tests cover `psql`-style scanning behavior
    - stable output verified in replay-safe tests

- [x] Add regression suite for history shape stability.
  - Snapshot expected column metadata and result shape.
  - Definition of done:
    - deterministic test fixtures cover inserts, updates, deletes
    - CI fails on accidental contract drift

- [x] Document history semantics clearly.
  - Include metadata glossary and usage examples.
  - Definition of done:
    - docs updated
    - one end-to-end example added to cookbook or README

## Epic X — Temporal introspection primitives (`P0`)

### Goal

Give developers supported ways to inspect row, entity, and snapshot state without scraping internal behavior.

### Tasks

- [x] Design minimal supported introspection surface.
  - Candidate functions:
    - `current_lsn()`
    - `row_lsn(...)`
    - `entity_version(...)`
    - `entity_head_lsn(...)`
    - `resolve_reference(...)`
  - Definition of done:
    - API/design note written
    - naming and return types approved

- [x] Implement `current_lsn()`.
  - Definition of done:
    - callable through supported SQL path
    - deterministic tests added
    - docs updated

- [x] Implement row-level temporal lookup helper.
  - Start with single-row primary-key lookup semantics.
  - Definition of done:
    - clear not-found behavior documented
    - tests cover current and historical cases

- [x] Implement entity-level version lookup helper.
  - Definition of done:
    - entity head/version semantics documented
    - tests cover versioned entity tables

- [x] Expose these helpers through pgwire and SDK examples.
  - Definition of done:
    - example queries committed
    - client roundtrip verified in integration tests

## Epic Y — Versioned foreign key ergonomics (`P0`)

### Goal

Keep temporal correctness while removing manual raw `LSN` work from ordinary writes.

### Tasks

- [x] Specify automatic versioned-reference capture semantics.
  - Decide when visible referenced row versions are resolved.
  - Define failure behavior if a reference is not visible.
  - Definition of done:
    - spec covers `INSERT`, `UPDATE`, same-tx visibility, and replay implications

- [x] Implement automatic `*_lsn` population for normal writes.
  - Use transaction-visible snapshot to resolve reference versions.
  - Definition of done:
    - normal inserts succeed without manual reference `LSN`
    - deterministic replay tests pass
    - same-transaction reference scenarios covered

- [x] Add explicit override mode for advanced workflows.
  - Preserve precise temporal control when needed.
  - Definition of done:
    - override syntax or API documented
    - validation errors are explicit

- [x] Add diagnostics for resolved reference versions.
  - Make it easy to inspect what the engine captured.
  - Definition of done:
    - supported query/debug path exists
    - tests cover visibility and mismatch cases

- [x] Add integration seed scenario that previously required manual `LSN` plumbing.
  - Use invoices/admissions or similar realistic case.
  - Definition of done:
    - scenario passes end-to-end with simpler SQL

## Epic Z — SQL and pgwire compatibility policy (`P1`)

### Goal

Eliminate ambiguity about what PostgreSQL-like behavior ASQL does and does not promise.

### Tasks

- [x] Publish compatibility stance.
  - Choose between pragmatic subset and explicit ASQL-native profile.
  - Definition of done:
    - short policy doc exists
    - linked from README and protocol docs

- [x] Define supported DML/query patterns for common app workflows.
  - Include prepared params, `IN`, ordering, pagination, and high-value selection patterns.
  - Definition of done:
    - matrix includes supported, unsupported, and planned items

- [x] Add compatibility tests for declared supported patterns.
  - Definition of done:
    - tests cover at least the published subset
    - CI highlights regressions clearly

- [x] Add guardrails for unsupported but likely PostgreSQL-assumed patterns.
  - Prefer explicit errors over surprising behavior.
  - Definition of done:
    - unsupported cases fail with actionable messages where practical

## Epic AA — Deterministic fixtures and seed workflows (`P1`)

### Goal

Make fixtures a first-class strength of ASQL.

### Tasks

- [x] Define fixture format and lifecycle.
  - Decide scope: domain, entity, whole scenario.
  - Decide whether IDs, timestamps, and versions can be controlled explicitly.
  - Definition of done:
    - format/design doc written

- [x] Implement minimal fixture import path.
  - Prioritize deterministic local/demo workflows first.
  - Definition of done:
    - can load a small multi-domain scenario reproducibly
    - validation errors are explicit

- [x] Implement fixture validation command or workflow.
  - Verify references, ordering, and deterministic assumptions.
  - Definition of done:
    - fixture validation catches broken references before apply

- [x] Implement minimal export path for debugging and reproducibility.
  - Definition of done:
    - export of a stable scenario or entity set works
    - roundtrip test exists

- [x] Add one benchmark/demo fixture pack.
  - Use a realistic reference scenario.
  - Definition of done:
    - fixture can initialize a reproducible demo/benchmark environment

## Epic AB — Aggregate-first application surface (`P2`)

### Goal

Align more of the public model with business aggregates, entities, and lifecycle snapshots.

### Tasks

- [x] Write design note on entity-version and aggregate-reference semantics.
  - Ground it in healthcare/manufacturing/finance examples.
  - Definition of done:
    - note reviewed and linked from architecture docs

- [x] Identify one aggregate-friendly API or SQL primitive for pilot implementation.
  - Keep scope narrow and testable.
  - Definition of done:
    - pilot chosen with acceptance criteria

- [x] Add one vertical slice using aggregate-oriented semantics.
  - Prefer a workflow already painful with row-plus-`LSN` handling.
  - Definition of done:
    - end-to-end example implemented and documented

## Suggested order of execution

1. Epic W — history contract stabilization
2. Epic X — temporal introspection primitives
3. Epic Y — versioned foreign key ergonomics
4. Epic Z — SQL and pgwire compatibility policy
5. Epic AA — deterministic fixtures and seed workflows
6. Epic AB — aggregate-first application surface

## Acceptance gates for closing this backlog

- [x] A realistic multi-domain seed can run without manual raw `LSN` plumbing in the ordinary path.
- [x] `FOR HISTORY` has a stable tested contract.
- [x] Temporal introspection helpers are documented and supported.
- [x] Compatibility expectations are explicit and regression-tested.
- [x] Fixture workflows are usable for demos, tests, and reproducible scenarios.

## Related documents

- [docs/asql-engine-dx-and-versioned-fk-improvement-plan-v1.md](asql-engine-dx-and-versioned-fk-improvement-plan-v1.md)
- [docs/aggregate-reference-semantics-v1.md](aggregate-reference-semantics-v1.md)
- [docs/adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md](adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md)
- [docs/ai/05-backlog.md](ai/05-backlog.md)
