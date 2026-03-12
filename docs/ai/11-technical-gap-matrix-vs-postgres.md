# ASQL Technical Gap Matrix — vs PostgreSQL (post-Epic U)

Date: 2026-03-01

Status note (2026-03-12): this matrix is useful for strategic gap framing, but active sequencing should be taken from [docs/ai/05-backlog.md](05-backlog.md).

## Scope and intent

This document prioritizes what to build next to improve technical competitiveness against PostgreSQL while preserving ASQL's wedge:

- deterministic replay and state equivalence,
- domain isolation and explicit cross-domain boundaries,
- audit/time-travel operational experience.

The goal is not full parity; it is to close the most credibility-critical technical gaps first.

---

## Current differentiation (where ASQL is stronger)

1. Deterministic replay as a first-class contract (audit/debug strength).
2. Domain-isolated transactional model inside one engine.
3. Explicit cross-domain protocol over implicit side effects.
4. Time-travel and replay primitives integrated in core/admin paths.
5. Deterministic scan strategy instrumentation and query-path observability.

---

## Status update since prior matrix

Completed (Epic R + Epic S):
- deterministic `UPDATE` and `DELETE`,
- `PRIMARY KEY` + `UNIQUE` deterministic conflict behavior,
- SQL NULL three-valued logic baseline (`IS NULL` / `IS NOT NULL`),
- deterministic aggregation floor (`COUNT`, `SUM`, `AVG`, `GROUP BY`, `HAVING`),
- MVCC snapshot baseline + deterministic write-write conflict detection,
- `FOREIGN KEY` + `CHECK` subset,
- boolean/arithmetic expression subset for `WHERE`/`HAVING`,
- composite `BTREE` indexes,
- lightweight deterministic cost-guided scan strategy selection,
- savepoint baseline (`SAVEPOINT`, `ROLLBACK TO`).

Completed since matrix refresh (S+ incremental):
- replication leader `LastLSN` RPC,
- lag helper baseline in replication module,
- ASQL Studio replication panel with leader/follower lag visibility,
- read-only query path with deterministic lag-aware routing policy (`strong` / `bounded-stale`),
- routing/staleness telemetry counters exposed in Studio.

Completed since matrix refresh (U incremental):
- deterministic domain-group failover flow (`leader_down -> candidate_elected -> promoted_leader`) with serialized transitions,
- write gating with fencing token checks on commit path,
- integration failure simulations (leader crash, delayed heartbeat, dual-candidate contention, stale leader recovery),
- replay/state-hash continuity verification across promotion,
- observability coverage for failover transitions and fencing rejections,
- deterministic repeated failover winner/term sequence checks.

Implication:
- credibility gaps moved from SQL baseline/concurrency fundamentals and HA baseline toward compatibility breadth, optimizer depth, reusable read-routing beyond Studio-specific UX, and migration/runtime ecosystem.

---

## Remaining gap matrix (impact × effort × risk)

Scale:
- Impact: 1 (low) to 5 (high)
- Effort: 1 (low) to 5 (high)
- Risk: 1 (low) to 5 (high)
- Priority score: `(Impact * 2) - Effort - Risk`

| Gap | Impact | Effort | Risk | Score | Why it matters now |
|---|---:|---:|---:|---:|---|
| Replica-read packaging and SDK ergonomics | 4 | 3 | 2 | 3 | Shared routing exists, but production teams still need easier client adoption, clearer policy defaults, and stronger operator guardrails. |
| Optimizer depth (join order, multi-index choices, plan stability) | 5 | 5 | 4 | 1 | Required for predictable latency under larger real schemas/workloads. |
| Broader PostgreSQL driver/tool compatibility | 5 | 5 | 4 | 1 | Largest adoption friction: tools and apps expect deeper Postgres interoperability than the current pragmatic subset. |
| Secondary index breadth (covering/partial/function-like paths) | 4 | 4 | 3 | 1 | Critical for complex query shapes without full scans. |
| Online schema evolution and migration ergonomics | 4 | 4 | 3 | 1 | Real-world teams need low-risk schema changes and rollout controls. |
| Security/compliance runtime depth (RLS-style controls, richer auditing policies) | 4 | 4 | 3 | 1 | Important in regulated industries and multi-team operations. |
| Extended SQL coverage (subqueries, more joins, window funcs, DDL breadth) | 4 | 5 | 4 | -1 | Major parity gap for migration from mature PostgreSQL workloads. |

Priority order for next 8 weeks:
1) Improve replica-read packaging, SDK ergonomics, and operator guardrails
2) Optimizer depth increment (deterministic join/index strategy expansion + explainability)
3) PostgreSQL protocol compatibility spike (narrow but real tool interoperability slice)
4) Online schema evolution + migration ergonomics baseline

---

## Recommended next 8-week technical roadmap

## Sprint T1 (Weeks 1–2): Replica-read packaging and operator ergonomics

Deliverables:
- tighten reusable lag-aware routing defaults for service and SDK consumers,
- expose consistency-window metadata and routing decisions more clearly for clients/operators,
- add deterministic tests for identical routing outcomes under identical lag/state inputs.

Acceptance gates:
- routing decisions are deterministic across repeated seeded lag timelines,
- stale-read boundary and leader fallback behavior are clearly API-visible and SDK-consumable,
- operator telemetry includes route decisions and fallback reasons.

## Sprint T2 (Weeks 3–4): Optimizer depth increment

Deliverables:
- deterministic join strategy expansion for supported multi-table shapes,
- stronger index candidate selection for competing paths,
- `EXPLAIN`/plan diagnostics output for operator transparency.

Acceptance gates:
- benchmark deltas on representative workloads,
- deterministic plan-selection snapshots remain stable,
- no replay divergence introduced by planner changes.

## Sprint T3 (Weeks 5–6): Compatibility wedge (PostgreSQL protocol spike)

Deliverables:
- PostgreSQL wire/protocol compatibility spike for narrow SQL subset.
- validate one mainstream Postgres client/tool roundtrip against ASQL.
- document explicit compatibility boundaries and deterministic behavior contract.

Acceptance gates:
- successful roundtrip from external Postgres-compatible client,
- deterministic behavior preserved in compatibility mode,
- unsupported-surface contract published.

## Sprint T4 (Weeks 7–8): Migration ergonomics + schema evolution baseline

Deliverables:
- online-safe schema evolution primitives for practical rollout workflows,
- migration ergonomics improvements for SQLite/Postgres-lite paths,
- guardrails/checks for deterministic migrations and rollback safety.

Acceptance gates:
- schema evolution workflows validated in integration tests,
- migration runbook includes deterministic rollback/verification steps,
- no regressions in determinism acceptance suite.

---

## Non-goals for this 8-week window

- Full PostgreSQL SQL dialect parity.
- Full cost-based optimizer with advanced join reordering.
- Protocol-level drop-in PostgreSQL wire compatibility.

---

## Technical risks and mitigations

1. Semantic regressions while adding SQL surface.
   - Mitigation: deterministic acceptance suite as release blocker for every sprint.

2. Performance regressions from correctness-first implementations.
   - Mitigation: keep strategy counters and benchmark pack mandatory per sprint.

3. Scope creep toward full parity.
   - Mitigation: enforce sprint non-goals and acceptance gates exactly as listed.

---

## Decision summary

After closing Epic R/S/T and Epic U acceptance gates, ASQL has a credible deterministic HA baseline and should shift competitive execution toward adoption friction reducers and workload predictability: promote replica-read policy to reusable service/API paths, deepen deterministic optimizer behavior, deliver a narrow PostgreSQL compatibility wedge, and improve migration/schema-evolution ergonomics.

Execution mapping:
- Backlog implementation track is captured in `docs/ai/05-backlog.md` under **Epic V — Post-Epic U competitiveness execution (8-week)**.
