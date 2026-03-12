# ASQL Engine DX and Versioned Foreign Keys Improvement Plan v1

> Historical note
>
> This plan was an important input into later productization work, but it should now be read as background context rather than the current plan of record.
>
> Current source-of-truth docs are:
>
> - [docs/getting-started/README.md](getting-started/README.md)
> - [docs/asql-adoption-friction-prioritized-backlog-v1.md](asql-adoption-friction-prioritized-backlog-v1.md)
> - [docs/ai/05-backlog.md](ai/05-backlog.md)
> - [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](adr/0002-generalist-engine-boundary-and-adoption-surface.md)

## Purpose

This document captures practical findings from building and seeding the Hospital MiKS demo on top of ASQL. It focuses on the developer experience, SQL ergonomics, versioned foreign keys, history introspection, and fixture/seeding workflows.

The goal is not to question ASQL's core direction. The goal is to identify where the current product surface leaks engine internals and makes normal application development harder than it should be.

## Executive summary

ASQL's architectural direction remains strong:

- domain isolation is valuable,
- deterministic WAL and replay are differentiated,
- time-travel and audit are strategically important,
- entity versioning is aligned with compliance-heavy workloads.

The main problem is **surface design**, not core intent.

Today, ASQL exposes too much of its storage/runtime model to application developers. In particular:

- versioned foreign keys require low-level knowledge of LSNs,
- the engine does not provide ergonomic APIs to resolve current reference versions,
- `FOR HISTORY` is not stable enough as a developer-facing contract,
- SQL compatibility feels close enough to PostgreSQL to create expectations, but diverges in important places,
- realistic demo-data seeding becomes much harder than it should be.

In short:

> ASQL currently asks application developers to think like the engine, when it should allow them to think like the domain.

## Context and trigger

These findings emerged while trying to build richer longitudinal hospital workflows:

- multiple domains,
- cross-domain references,
- historical patient flows,
- invoices linked to admissions,
- documents, rehab, messaging, pharmacy, scheduling,
- realistic demo/seed datasets for UI and product testing.

This surfaced several friction points that would affect any serious ASQL application, not just Hospital MiKS.

## What is working well

These parts should be preserved and strengthened:

### 1. Domain isolation

The `BEGIN DOMAIN <name>` model is coherent and supports bounded-context design well.

### 2. Deterministic WAL / replay model

The replay-first philosophy is strategically correct for:

- compliance,
- auditability,
- offline/edge,
- deterministic debugging,
- event-driven backends.

### 3. Time-travel and history

The concepts behind `AS OF LSN` and `FOR HISTORY` are strong product differentiators.

### 4. Entity versioning

The idea of treating some tables as aggregates is directionally correct, especially for:

- admissions,
- prescriptions,
- invoices,
- patient-centric episodes,
- manufacturing-style controlled processes.

### 5. Opinionated single-node-first foundation

The product remains strongest when it stays compact, deterministic, and explicit.

## Core problems

## Problem 1: Versioned foreign keys are conceptually strong but operationally awkward

Current VFK design introduces friction because normal inserts may require the caller to understand and provide low-level reference timing metadata.

In practice, this means application code or seed tooling may need to know:

- which referenced row version is visible,
- which LSN should be used,
- when a referenced row was committed,
- how to retrieve that metadata reliably.

This is too much engine detail for normal business logic.

### Why this is a problem

A versioned reference is a good capability.
Requiring application authors to manually manage reference LSNs is not.

### Root cause

ASQL currently exposes the persistence mechanism (`LSN`) instead of a higher-level reference abstraction.

## Problem 2: LSN is leaking into the developer experience

`LSN` is a storage/log concern.
It is useful internally and for advanced observability, but it should not be the primary unit that application developers manipulate for common workflows.

### Symptoms

- seeding becomes fragile,
- cross-domain inserts become procedural,
- code must infer or fetch internal commit positions,
- product developers are pushed into engine-level reasoning.

### Better abstraction

Application-facing workflows should prefer:

- `CURRENT` reference resolution,
- entity versions,
- snapshot semantics,
- explicit point-in-time references only when necessary.

## Problem 3: Missing ergonomic introspection APIs

ASQL has the concepts needed for historical correctness, but lacks simple primitives for developers to use them safely.

### Missing capabilities

Examples of missing or insufficiently discoverable primitives:

- `current_lsn()`
- `row_lsn(domain, table, id)`
- `entity_version(domain, entity, id)`
- `resolve_reference(domain.table, id)`
- `resolve_reference_as_of(...)`

Without these, developers must improvise using lower-level features such as `FOR HISTORY`, which is not an acceptable default workflow.

## Problem 4: `FOR HISTORY` is not yet a stable product contract

The documentation presents `FOR HISTORY` as a first-class feature, but practical usage reveals instability in shape, naming, or scanning expectations.

### Why this matters

History/audit is not a side feature in ASQL.
It is part of the value proposition.

Therefore its contract must be:

- explicit,
- documented,
- stable,
- machine-friendly,
- consistent across pgwire and SDK usage.

### Required quality bar

A developer should know exactly:

- which metadata columns exist,
- what they are called,
- which types they return,
- whether nulls are possible,
- whether the shape is guaranteed over time.

## Problem 5: PostgreSQL compatibility expectations are not managed tightly enough

ASQL speaks over pgwire and uses SQL that looks close to PostgreSQL.
That creates a strong expectation of pragmatic compatibility.

When common PostgreSQL patterns behave differently, developer trust drops quickly.

### Typical danger zone

- parameter behavior,
- array-like patterns such as `ANY($1)`,
- metadata column exposure,
- tooling assumptions in `psql`, ORMs, and migration scripts,
- scan semantics.

### Product risk

The worst position is to be:

- similar enough to PostgreSQL to attract PostgreSQL mental models,
- different enough in edge behavior to break common workflows.

ASQL should choose a clear stance:

1. **Pragmatic PostgreSQL subset** with strong compatibility guarantees for common app workflows, or
2. **Distinct SQL/data platform** with explicit ASQL-native tooling and expectations.

The current middle ground is expensive.

## Problem 6: Realistic seeding and fixtures are too hard

For a system built around determinism, replay, audit, and domain boundaries, fixture handling should be a strength.

Instead, realistic seed workflows currently become labor-intensive because they must deal with:

- domain ordering,
- reference timing,
- VFK bookkeeping,
- inconsistent compatibility assumptions,
- cross-domain demo scenarios.

### Why this matters strategically

Fixtures are not just a developer convenience.
They are central to:

- product demos,
- end-to-end testing,
- determinism verification,
- onboarding,
- benchmark reproducibility,
- design-partner evaluation.

## Problem 7: The engine is closer to rows and LSNs than to aggregates and episodes

For applications like Hospital MiKS, the product model is moving toward:

- patient flows,
- episodes of care,
- longitudinal journeys,
- aggregate lifecycle tracking,
- cost attribution by episode.

This maps better to:

- entities,
- aggregate versions,
- explicit business references,

than to:

- row IDs plus raw LSNs.

ASQL should move more of its application-facing surface toward business aggregates rather than storage primitives.

## Design principles for improvement

## 1. Keep engine internals available, but not mandatory

Advanced users should still be able to work with raw LSNs.
Common workflows should not require it.

## 2. Prefer automatic reference resolution by default

If a transaction sees a referenced row, the engine should be able to capture the correct reference version automatically unless the caller explicitly overrides it.

## 3. Prefer entity versions over raw LSNs in application-facing APIs

LSNs are operational.
Entity versions are conceptual.
ASQL should expose both, but bias higher-level workflows toward entity versions.

## 4. Make history and time-travel contractual

These features should be reliable enough to power SDKs, admin tooling, and production diagnostics without reverse-engineering.

## 5. Make deterministic fixtures a first-class workflow

A deterministic database should provide best-in-class fixture support.

## Proposed improvements

## Proposal A: Automatic VFK capture on insert/update

When inserting a row with a versioned FK, the engine should support this workflow:

```sql
INSERT INTO invoices (patient_id, admission_id, ...)
VALUES ('patient-1', 'admission-1', ...);
```

The engine should automatically resolve:

- `patient_lsn`
- `admission_lsn`

from the current visible snapshot inside the transaction.

### Benefits

- business logic becomes simple,
- seeds become much easier,
- SQL becomes more natural,
- the feature becomes usable outside engine experts.

### Optional advanced mode

Allow explicit override only when needed:

```sql
INSERT INTO invoices (...)
VALUES (...)
REFERENCING SNAPSHOT EXPLICIT;
```

or equivalent syntax.

## Proposal B: Introduce higher-level reference semantics

Instead of centering references on raw LSNs, add product-level semantics such as:

- `CURRENT`
- `AS OF LSN <n>`
- `AS OF VERSION <n>`
- `AT SNAPSHOT`

Example direction:

```sql
REFERENCES patients.patients(id) AT CURRENT
REFERENCES clinical.admissions(id) AT CURRENT
```

or

```sql
REFERENCES clinical.admissions(id) AT VERSION 7
```

This makes the model understandable without hiding the temporal guarantees.

## Proposal C: Add introspection functions

Provide officially supported query helpers such as:

- `current_lsn()`
- `row_lsn('domain.table', 'id')`
- `row_exists_as_of(...)`
- `entity_version('domain', 'entity', 'id')`
- `entity_head_lsn('domain', 'entity', 'id')`

### Purpose

- tooling,
- debugging,
- admin UI,
- migrations,
- fixtures,
- deterministic test harnesses.

## Proposal D: Stabilize `FOR HISTORY`

Define and lock a stable output contract.

### Minimum requirements

- stable metadata column names,
- stable types,
- documented nullability,
- examples for `psql`, Go SDK, and pgwire clients,
- guarantee whether row images are current-value-only or old/new-style.

### Recommended shape

Prefer an explicit output like:

- `__operation`
- `__commit_lsn`
- `__commit_ts` (if available conceptually)
- user columns

or formally document any different shape and guarantee it.

## Proposal E: Clarify PostgreSQL compatibility policy

Publish a concise compatibility contract.

### Suggested options

#### Option 1 — Compatible subset

Document a supported PostgreSQL-oriented subset covering:

- prepared parameters,
- common DML/DDL,
- common `SELECT` patterns,
- `IN`, arrays, `ANY`, ordering, pagination,
- `psql` usability expectations.

#### Option 2 — Explicit ASQL dialect profile

State that ASQL is not aiming for broad PostgreSQL behavioral compatibility and provide:

- ASQL-native docs,
- CLI tooling,
- fixture loader,
- recommended query patterns,
- compatibility lints/warnings.

Either path is acceptable. Ambiguity is not.

## Proposal F: First-class fixtures and deterministic seeds

Introduce native fixture support.

### Possible capabilities

- `LOAD FIXTURE <file>`
- `EXPORT FIXTURE <domain|entity>`
- `UPSERT FIXTURE`
- fixture references resolved automatically across domains
- deterministic timestamp control
- deterministic UUID control
- snapshot-aware imports

### Why this fits ASQL especially well

This aligns directly with:

- replay,
- reproducibility,
- demo environments,
- benchmark reproducibility,
- scenario testing.

## Proposal G: Move application-facing modeling toward entities/episodes

The more ASQL wants to serve domains like healthcare, manufacturing, finance, or regulated workflows, the more important it becomes to expose aggregate-friendly semantics.

Instead of pushing application authors toward:

- rows,
- table-level history,
- raw LSN management,

ASQL should help them model:

- episodes,
- aggregates,
- business snapshots,
- lifecycle versions.

## Recommendation for Hospital MiKS specifically

Hospital MiKS should eventually reference explicit business flows/episodes, not infer everything from raw table groupings.

ASQL can support that well if it offers:

- aggregate version references,
- snapshot-safe links,
- better fixture tooling,
- easier cross-domain temporal references.

## Suggested roadmap

## Phase 1 — Immediate DX fixes

1. stabilize `FOR HISTORY`
2. expose current row/entity LSN helpers
3. document SQL compatibility boundaries clearly
4. support simpler common parameter/query patterns where possible

## Phase 2 — VFK ergonomics

1. auto-populate VFK `*_lsn` fields on insert/update
2. allow explicit override only for advanced cases
3. provide inspection helpers for resolved reference versions

## Phase 3 — Fixtures/tooling

1. deterministic fixture import/export
2. multi-domain seed support
3. snapshot-aware demo tooling
4. CLI support for fixture validation

## Phase 4 — Aggregate-first product surface

1. entity version references as first-class primitives
2. aggregate-aware APIs and documentation
3. better alignment between engine abstractions and business workflows

## Concrete decisions proposed

## Decision 1

Keep versioned foreign keys.
Do not remove them.

## Decision 2

Stop requiring raw LSN management in normal application workflows.

## Decision 3

Introduce automatic reference-version capture at write time.

## Decision 4

Treat `FOR HISTORY` as a stable public contract, not an implementation detail.

## Decision 5

Treat fixtures/seeds as first-class ASQL capabilities.

## Decision 6

Bias application-facing design toward entity versions and business aggregates rather than raw row LSNs.

## Risks if nothing changes

- application development remains slower than it should be,
- demos and reference apps become harder to maintain,
- design partners hit avoidable friction early,
- product perception becomes “interesting engine, difficult to use”,
- PostgreSQL expectations continue to cause confusion,
- internal teams accumulate workaround logic outside the engine.

## Success criteria

A healthy future state would look like this:

- a normal application can seed realistic multi-domain data without engine-specific hacks,
- most application code never directly touches raw LSNs,
- versioned references are safe but ergonomic,
- `FOR HISTORY` works reliably in both tooling and application code,
- SQL compatibility expectations are explicit and dependable,
- aggregate/episode-centric apps feel natural to build on ASQL.

## Final diagnosis

ASQL does not appear to have the wrong core architecture.

The main gap is that the current public surface is too close to the engine's internal mechanics.

The right next step is not to remove determinism, domains, or temporal references.
The right next step is to **wrap those capabilities in a much better developer-facing model**.

That means:

- less raw LSN handling,
- better reference resolution,
- stronger contracts,
- better introspection,
- native fixture workflows,
- more aggregate-first semantics.
