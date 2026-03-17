# ASQL adoption roadmap v1

Status: active product/planning snapshot as of 2026-03-17.

This roadmap is the adoption-oriented companion to the active execution backlog in [docs/ai/05-backlog.md](../ai/05-backlog.md).
Use the backlog for the next concrete engineering slice; use this roadmap to keep sequencing and scope aligned.

## Goal

Make ASQL feel like:
- a pragmatic PostgreSQL-compatible engine for mainstream tools and app flows,
- a clearly differentiated database for deterministic replay and temporal debugging,
- an operator-safe system with strong visibility around failover, lag, backup, and recovery,
- an easy product to evaluate locally and adopt incrementally.

## Non-negotiable constraints

- Single-node first.
- Determinism first.
- WAL remains canonical.
- pgwire remains the canonical app-facing runtime.
- PostgreSQL compatibility stays selective and adoption-driven, not parity-driven.
- Temporal/history features must remain general-purpose database primitives, not app-specific workflow semantics.

## Strategic bets

### 1. Mainstream PostgreSQL wedge
Win the first evaluation loop by making more existing drivers, tools, and simple apps work without surprises.

### 2. Planner and performance credibility
Win trust by making query behavior observable, benchmarked, and easier to reason about.

### 3. Temporal superpower
Win differentiation by making history, replay, and point-in-time debugging materially better than the default market baseline.

### 4. Operator-grade boringness
Win production trust by making health, lag, failover, backup, and restore easy to inspect and drill.

### 5. Fast adoption loop
Win teams by shrinking the path from “interesting project” to “working local evaluation” and then to “safe pilot”.

## 12-month sequence

## Q2 2026 — adoption wedge and credibility floor

Primary objective:
- remove the biggest reasons a PostgreSQL-oriented team bounces during evaluation.

Focus:
- PostgreSQL-compatible flows that unblock mainstream tools, drivers, and simple app usage.
- high-return SQL/planner improvements tied to observed adoption friction.
- benchmark-backed planner and query credibility.

Success signals:
- more mainstream client/tool startup and metadata flows work in the documented subset,
- query plans are easier to inspect and trust,
- benchmark evidence exists for the most common query and mutation shapes.

## Q3 2026 — temporal and operator differentiation

Primary objective:
- make ASQL obviously stronger in historical debugging and operational clarity.

Focus:
- temporal explainability and snapshot-diff workflows,
- operator-grade cluster/recovery workflows in Studio, CLI, and docs,
- stronger failover and long-running write-path behavior under leader change.

Success signals:
- temporal workflows are easy to demonstrate and teach,
- operator surfaces explain failover, lag, replay, and recovery without internal knowledge,
- ASQL has a differentiated story beyond “smaller Postgres”.

## Q4 2026 — migration and production proof

Primary objective:
- convert strong evaluation into credible pilot readiness.

Focus:
- migration starter kits and translation guidance,
- benchmark, reliability, and recovery proof loops,
- tighter packaging of examples, templates, and production readiness guidance.

Success signals:
- teams can evaluate migration effort quickly,
- production pilot criteria are concrete and test-backed,
- the product surface feels coherent across README, getting-started, reference, Studio, and CLI.

## Execution mapping to backlog epics

- Epic AI: mainstream PostgreSQL app/tool adoption wedge
- Epic AJ: planner and performance credibility
- Epic AK: temporal superpower workflows
- Epic AL: operator-grade production boringness
- Epic AM: migration and onboarding compression

## Things to avoid

Do not spend the next window on:
- broad PostgreSQL parity for low-value edge cases,
- vertical workflow semantics that belong in applications,
- clustering complexity that weakens single-node clarity,
- feature sprawl without docs, tests, and operator visibility.
