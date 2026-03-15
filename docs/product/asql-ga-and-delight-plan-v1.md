# ASQL GA + Delight Plan (v1)

Date: 2026-03-15
Status: active prioritization snapshot for the next execution window.

This document translates the current product posture into a concrete rule for what to do next so ASQL becomes not just technically credible, but genuinely excellent to adopt and operate.

It complements, but does not replace:
- [docs/ai/05-backlog.md](../ai/05-backlog.md) for agent-executable backlog ordering,
- [docs/product/production-readiness-roadmap-v1.md](production-readiness-roadmap-v1.md) for broader readiness context,
- [README.md](../../README.md), [docs/getting-started/README.md](../getting-started/README.md), and [docs/reference/](../reference/) for external truth surfaces.

## Executive summary

ASQL does **not** need the broadest feature list to become compelling.
It needs to become:

1. **boringly reliable**,
2. **predictably fast on its main write/read paths**,
3. **easy to understand and adopt through pgwire**,
4. **operator-friendly for replay, failover, and historical debugging**,
5. **explicit about what PostgreSQL compatibility means and does not mean**.

The shortest path to a "wonderful" ASQL is therefore:
- freeze the compatibility contracts that matter,
- turn performance/reliability expectations into release gates,
- make the canonical onboarding/runtime path frictionless,
- make ASQL Studio and operator docs expose the product's unique strengths,
- expand compatibility only where it removes mainstream adoption friction.

## Product standard to optimize for

ASQL should aim to be the easiest database in its wedge to:
- reason about,
- replay,
- audit,
- debug historically,
- and operate under failover.

That wedge remains:
- explicit domain isolation,
- append-only truth via WAL,
- deterministic replay,
- time-travel and auditability,
- practical clustered operation over pgwire + Raft.

## What "great" looks like

ASQL is in a great state when all of the following are true:

### 1. Contract stability is explicit
- WAL compatibility policy is written, versioned, and release-gated.
- pgwire/protocol compatibility claims are documented and test-backed.
- snapshot/recovery expectations are documented as contracts, not implementation accidents.

### 2. Performance regressions are hard to reintroduce
- single-node write scaling guardrails are part of release validation,
- cluster append-growth and quorum-write guardrails are part of release validation,
- p95/p99 envelopes for canonical workloads are documented and tracked release-over-release.

### 3. The main user path feels polished
- `cmd/asqld` is the canonical local runtime,
- `cmd/asqlctl` covers the main operational and inspection workflows,
- ASQL Studio gives useful visibility into leader/replica behavior, LSN state, temporal reads, and failover posture,
- the docs portal and examples point to the same canonical path with minimal duplication.

### 4. Adoption friction is low for mainstream clients
- `pgx`, `psql`, and mainstream metadata/startup flows work reliably within the documented subset,
- SQLSTATE and row/parameter descriptions are stable enough for normal tooling,
- unsupported areas are documented clearly rather than discovered by surprise.

### 5. The unique value is visible everywhere
- README, getting started docs, Studio, and benchmark sheets all reinforce the same message:
  - domains,
  - deterministic replay,
  - time-travel,
  - observable failover and lag,
  - operational clarity.

## Priority order

### Priority 1 — GA contracts and release gates

This is the most important work.

#### Scope
- Freeze WAL/protocol compatibility rules for GA.
- Define release-candidate gates for:
  - replay equivalence,
  - upgrade/backward-compat validation,
  - write-scaling guardrails,
  - cluster append-growth guardrails,
  - failover continuity,
  - compatibility test packs.
- Treat benchmark baselines as release evidence, not just engineering notes.

#### Why first
Without this, ASQL can look impressive but still feel risky.
The fastest way to increase trust is to make reliability, determinism, and non-regression visible and enforceable.

#### Acceptance criteria
- A release candidate cannot ship if compatibility or performance gates fail.
- Public docs state what is guaranteed across upgrades and what is not.
- Benchmark evidence is reproducible from repository scripts/tasks.

### Priority 2 — Canonical docs + examples surface

#### Scope
- Finish the docs portal and examples repo plan around the canonical path:
  - `asqld`,
  - `asqlctl`,
  - ASQL Studio with `-pgwire-endpoint`.
- Ensure README, getting started docs, reference docs, and site pages stay aligned.
- Organize examples around adoption moments, not feature dumps.

#### Why second
A strong engine with a fragmented story still feels immature.
The docs should make ASQL feel coherent on first contact.

#### Acceptance criteria
- A new user can go from zero to first successful domain transaction quickly.
- A real user can find examples for local start, schema setup, time-travel, fixtures, replication visibility, and failover inspection without reading unrelated product-planning docs.
- Public docs do not contradict one another about runtime paths or compatibility stance.

### Priority 3 — Studio and operator delight

#### Scope
ASQL Studio should emphasize the capabilities that make ASQL special:
- current head LSN and temporal navigation,
- replay/history visibility,
- leader/replica routing and lag state,
- failover/lease/fencing posture,
- scan strategy and query-path observability where useful.

#### Why third
This is where ASQL can feel qualitatively different from "yet another SQL frontend".

#### Acceptance criteria
- Operators can answer "where did this value come from?" and "which node is safe to read from?" from the default tooling.
- Temporal and cluster visibility are first-class workflows rather than hidden power features.

### Priority 4 — Selective PostgreSQL compatibility friction removal

#### Scope
Keep compatibility work demand-driven and adoption-oriented:
- mainstream client startup and metadata flows,
- practical SQLSTATE fidelity,
- parameter/row description correctness,
- targeted catalog shims.

Do **not** pursue parity for its own sake.

#### Why fourth
Compatibility is valuable when it removes real friction, but dangerous when it expands surface area faster than reliability hardening.

#### Acceptance criteria
- Each new compatibility claim lands with docs + regression tests + compatibility matrix entry.
- Reported compatibility gaps are triaged as docs gap, protocol gap, catalog gap, SQL-surface gap, or explicit non-goal.

### Priority 5 — Ecosystem polish

#### Scope
- make `asqlctl` even more useful for real operator workflows,
- keep example apps realistic and opinionated,
- improve release packaging and evaluability.

#### Why fifth
This matters, but only after the system is stable, clear, and predictable.

## Explicit non-goals for this phase

Do not prioritize the following ahead of the items above:
- broad SQL parity for vanity reasons,
- vertical/business workflow semantics inside the engine,
- speculative parallel-query work without benchmarked need,
- platform sprawl that weakens determinism or operational clarity.

## Documentation contract for this phase

Whenever user-visible behavior changes in this phase, update the relevant truth surfaces together:
- [README.md](../../README.md) for the front-door summary,
- [docs/getting-started/](../getting-started/) for onboarding,
- [docs/reference/](../reference/) for precise contracts,
- [site/](../../site/) when public site pages mirror those claims,
- benchmark and operations docs when release gates or operator workflows change.

Rules:
- keep the README short,
- prefer improving existing getting-started pages over inventing parallel guides,
- avoid claiming full PostgreSQL parity,
- state the canonical pgwire path first whenever multiple server paths exist.

## Recommended execution sequence

### Wave 1 — trust and non-regression
1. freeze GA compatibility language,
2. formalize release gates for determinism/performance/compatibility,
3. keep benchmark guardrails green and documented.

### Wave 2 — first-contact coherence
1. finish docs portal structure,
2. finalize examples packaging,
3. make the main local path and daily workflow feel polished.

### Wave 3 — operator delight
1. prioritize Studio visibility for temporal and cluster workflows,
2. improve operator-facing CLI workflows,
3. refine observability and troubleshooting docs from real usage.

## How to decide whether work belongs in the current window

A change belongs in the current window if it makes ASQL more:
- predictable,
- testable,
- documentable,
- operationally obvious,
- or adoptable through the documented pgwire subset.

A change should usually wait if it only makes ASQL broader, more complex, or more parity-driven without improving trust or usability.

## Bottom line

ASQL becomes wonderful by becoming the most **understandable**, **replayable**, and **operationally explicit** database in its niche.

The next execution window should therefore optimize for:
- trust before breadth,
- release gates before claims,
- operator clarity before feature accumulation,
- and adoption coherence before parity chasing.
