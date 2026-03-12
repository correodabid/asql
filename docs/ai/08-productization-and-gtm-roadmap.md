# ASQL Productization + Go-To-Market Roadmap

Current state: technical MVP core is implemented (Epics A–J).

Status note (2026-03-12): this document is now a strategy snapshot rather than the active execution plan.
Use [docs/ai/05-backlog.md](05-backlog.md), [docs/product/production-readiness-roadmap-v1.md](../product/production-readiness-roadmap-v1.md), and the current categorized docs under [docs/getting-started/](../getting-started/) and [docs/operations/](../operations/) as the living sources of truth.

## Phase K — Production hardening (6–8 weeks)

### Goals
- Move from "works" to "reliable under load/failure".
- Establish quality and performance baselines.

### Workstreams
- Concurrency/race hardening (`go test -race` in CI).
- Crash/restart chaos tests for WAL + replay.
- Backward compatibility contract for WAL format + migration checks.
- Determinism stress suite (repeatability across many seeds/runs).

### Exit criteria
- P95/P99 latency and throughput baseline published.
- No P0/P1 determinism bugs open.
- Recovery and replay stability validated under fault injection.

## Phase L — Security + compliance baseline (4–6 weeks)

### Goals
- Minimum enterprise trust posture.

### Workstreams
- gRPC authn/authz (mTLS + token-based policy).
- Audit log events for tx lifecycle and admin operations.
- Security scanning in CI (SAST/dependency/CVE).
- Supply chain: signed builds + SBOM generation.

### Exit criteria
- Security checklist green for first customer pilots.
- Audit trail is queryable and replay-correlated.

## Phase M — Developer experience and packaging (4–6 weeks)

### Goals
- Make adoption frictionless for teams.

### Workstreams
- `asqlctl` promoted from demo tool to operational CLI:
  - tx/session inspect,
  - replay/time-travel commands,
  - replication status.
- Versioned Docker image and release artifacts.
- Public API examples (Go SDK first).
- "Getting started in 10 minutes" tutorial.

### Exit criteria
- New user can run local + replication demo in < 15 minutes.
- CLI covers top 80% operational workflows.

Implementation note:
- much of this phase has already been absorbed into current docs, tooling, and onboarding work; treat remaining bullets as packaging direction, not as an up-to-date gap list.

## Phase N — Beta program (6–8 weeks)

### Goals
- Validate product-market fit in real environments.

### Workstreams
- 3–5 design partners (edge, fintech/compliance, offline-first).
- Weekly telemetry-driven feedback loop.
- SLA/SLO draft and incident runbooks.
- Prioritized bug/feature triage lane.

### Exit criteria
- At least 2 production-like pilots stable for >30 days.
- Clear retained use-case with measurable ROI.

## Phase O — Commercial readiness (4–6 weeks)

### Goals
- Ship a professional offer, not only software.

### Workstreams
- Pricing model definition:
  - OSS core + paid enterprise features/support,
  - or commercial licensing with support tiers.
- Legal/package readiness:
  - license policy,
  - terms,
  - support policy,
  - vulnerability disclosure policy.
- Sales assets:
  - architecture one-pager,
  - benchmark sheet,
  - migration guide.

### Exit criteria
- Offer, packaging, and support process are publishable.

## Phase P — Launch (2–4 weeks)

### Goals
- Public release with confidence and repeatability.

### Workstreams
- Launch candidate (`v1.0.0-rc1`) + freeze window.
- Final regression, determinism, and replication gates.
- Public docs portal + examples repo.
- Announcement plan (website, technical post, demo video, community channels).

### Exit criteria
- `v1.0.0` GA released.
- Public onboarding and support channels live.

## KPIs to track from now

- Activation: time-to-first-successful-transaction.
- Reliability: crash recovery success rate.
- Determinism: replay equivalence pass rate.
- Performance: p95 write/read latency under reference workloads.
- Adoption: weekly active projects/nodes.
- Commercial: pilot-to-paid conversion rate.
