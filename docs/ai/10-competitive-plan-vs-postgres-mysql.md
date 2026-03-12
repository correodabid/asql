# ASQL Competitive Plan (6 Months) — vs PostgreSQL/MySQL

Date: 2026-02-28

Status note (2026-03-12): this document is a competitive strategy snapshot. For current execution priorities, use [docs/ai/05-backlog.md](05-backlog.md) and the categorized product/operations docs.

## Strategic position

ASQL should not compete first on full ANSI compatibility or ecosystem breadth. PostgreSQL/MySQL win there today.
ASQL should win first in a focused wedge:

- deterministic replay,
- audit-grade time-travel,
- domain-isolated transactional boundaries,
- edge/offline-first operational model.

The plan below balances **must-have parity** and **clear differentiation**.

## Execution status snapshot (as of 2026-02-28)

Completed at the time of this snapshot:
- Epic K (hardening) complete.
- Epic L (security baseline) complete.
- Epic M partially complete:
   - operational `asqlctl`,
   - Docker image + release pipeline,
   - 10-minute guide + onboarding smoke script.

Remaining critical path at the time of this snapshot:
- Epic M: Go SDK examples/cookbook.
- Epic N: SLO/runbooks/dashboards/pilot workflow.
- Epic O/P: commercial + launch packaging.

---

## Success criteria at Month 6

1. Two production-like pilots run >30 days with no P0 reliability incidents.
2. Published benchmark pack with transparent methodology and reproducible scripts.
3. Operational readiness: backup/restore, upgrade checks, SLO dashboards, runbooks.
4. Developer onboarding under 15 minutes from zero to first successful transaction.
5. Commercial readiness package (migration guide, support policy, architecture/benchmark one-pagers).

---

## Track A — Parity floor (must-have to be considered)

### A1. SQL and schema lifecycle
- Expand SQL subset to include practical join/filter/order/limit patterns used by app backends.
- Add schema migration safety checks and upgrade compatibility tests.
- Define explicit unsupported SQL matrix and deterministic alternatives.

### A2. Reliability and operations
- Backup/restore from WAL snapshots with validation checksum.
- Crash/restart chaos suite in CI (already started) expanded to long-run soak tests.
- WAL/tooling compatibility gates for every release candidate.

### A3. Platform expectations
- Docker image and release pipeline with signed artifacts and SBOM (partially done).
- Stable SDK examples (Go first, then Node/Python quickstarts).
- End-to-end smoke script for local and replicated topology.

---

## Track B — Differentiation moat (why pick ASQL)

### B1. Determinism guarantee as product feature
- Add deterministic state-hash checks in standard admin workflows.
- Provide replay verification command and report output for audit/compliance teams.
- Publish determinism SLA language (what is guaranteed, scope, and exceptions).

### B2. Domain isolation as architecture advantage
- Operational controls per domain (telemetry, quotas, error budgets).
- Cross-domain transaction observability with explicit coordinator trace IDs.
- Domain-scoped incident triage playbook.

### B3. Audit/time-travel experience
- First-class time-travel queries in CLI and SDK cookbook.
- Audit event query examples (security/compliance use cases).
- “Explain replay” diagnostics for failed historical reconstruction.

---

## 6-month phased execution

## Month 1–2 (Foundation to pilot-ready alpha)
- Close Epic M fully: Docker + release pipeline + cookbook + 10-minute guide.
- Baseline benchmark suite v1 (write/read/replay, p50/p95/p99).
- Backup/restore prototype and verification test.

Exit gate:
- First external design partner can install and run guided workflow in <15 min.

Current status:
- In progress and on track.
- Missing item: Go SDK cookbook + migration quick path docs.

## Month 3–4 (Pilot hardening)
- Close Epic N: SLO definitions, dashboards, incident runbooks, triage workflow.
- Long-run soak tests (24h+) for replay/replication stability.
- Upgrade and backward compatibility checklist automated in CI.

Exit gate:
- 1 pilot in production-like environment stable for >14 days.

## Month 5 (Commercial package)
- Close Epic O: pricing/licensing, support policy, vulnerability disclosure policy.
- Publish benchmark one-pager and architecture one-pager.
- Publish SQLite/Postgres-lite migration guide.

Exit gate:
- Sales/support artifacts are customer-facing and versioned.

## Month 6 (Launch candidate)
- Execute Epic P: `v1.0.0-rc1`, freeze, regression gates, docs portal, launch channels.
- Final consistency/completeness validation across docs + tests + CI artifacts.

Exit gate:
- GA go/no-go review passes with reliability + security + DX criteria green.

---

## KPI scoreboard (weekly)

Technical:
- Determinism pass rate (% replay equivalence passes)
- Crash recovery success rate
- Replication catch-up success rate
- p95 write/read latency (reference workload)

Product:
- Time-to-first-successful-transaction
- Docs-to-success conversion (% users completing quickstart)
- Pilot incident rate per week

Commercial:
- Pilot-to-paid conversion
- Time-to-resolution for support tickets

---

## Risks and mitigations

1. **Trying to match full PostgreSQL/MySQL feature-set too early**
   - Mitigation: strict scope discipline, publish supported SQL matrix.

2. **Benchmark claims challenged as non-comparable**
   - Mitigation: publish methodology, dataset, hardware profile, scripts.

3. **Determinism regressions under feature expansion**
   - Mitigation: deterministic acceptance suite as release blocker.

4. **Pilot friction due to packaging gaps**
   - Mitigation: prioritize Epic M completion before broader outreach.

---

## Immediate next 3 sprints

Sprint 1:
- Complete Docker + release pipeline
- Publish 10-minute guide + scripted smoke test

Sprint 1 status:
- Done.

Sprint 2:
- SDK cookbook (Go) + migration quick path from SQLite
- Backup/restore MVP with integrity validation

Sprint 2 status:
- Done.
- Completed deliverables:
   - Go cookbook and SQLite quick migration path.
   - Backup/restore MVP with SHA-256 integrity validation.
   - Integration parity test (`backup -> wipe -> restore -> query parity`).

Sprint 2 acceptance criteria:
- Cookbook includes at least 3 runnable recipes (`init`, `write`, `admin-check`).
- Migration quick path doc includes schema/data mapping caveats.
- Backup/restore MVP validated by integration test (`backup -> wipe -> restore -> query parity`).

Sprint 3:
- SLO dashboard baseline + incident runbook v1
- Pilot onboarding package v1

Sprint 3 status:
- Done.
- Completed deliverables:
   - SLO definitions v1
   - telemetry dashboard baseline v1
   - incident runbook v1
   - design-partner feedback triage workflow v1
   - release upgrade/backward-compat checklist v1

Sprint 3 acceptance criteria:
- SLO definitions published with alert thresholds.
- Dashboard includes replay lag, recovery success, and replication catch-up health.
- Runbook includes at least 3 incident scenarios with step-by-step remediation.

---

## KPI targets (next 8 weeks)

Technical targets:
- Determinism pass rate: >= 99.9% in CI deterministic suite.
- Crash recovery success: >= 99% in nightly chaos runs.
- Replication catch-up success: >= 99% in integration cycles.

Product targets:
- Time-to-first-successful-transaction: <= 15 minutes from clean machine.
- Smoke onboarding pass rate: >= 95% across supported dev environments.

Delivery targets:
- Sprint predictability: >= 80% committed tasks completed each sprint.