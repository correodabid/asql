# ASQL Release Notes — v1.0.0-rc1

Date: 2026-03-17
Status: release-notes draft for the first release candidate.
Scope: evaluator-facing summary aligned with the GA compatibility contract and launch narrative.

## Summary

ASQL `v1.0.0-rc1` is the first release candidate for the deterministic SQL engine built in Go.

This candidate is focused on trust, not feature sprawl:
- explicit domain isolation,
- replay-safe history and temporal inspection,
- deterministic replay from append-only WAL,
- practical pgwire-first application access,
- and optional clustered operation through pgwire + Raft.

ASQL supports a pragmatic PostgreSQL-compatible subset over pgwire for documented client and tool flows.
It is not a drop-in PostgreSQL replacement.

## Supported evaluation path

The recommended evaluation path for this release is:

1. start `cmd/asqld`,
2. connect through pgwire,
3. validate one local domain-scoped workflow,
4. try `AS OF LSN`, `FOR HISTORY`, and fixtures early,
5. inspect compatibility, benchmark, and production docs before widening assumptions,
6. use ASQL Studio through `-pgwire-endpoint` for temporal and operator workflows.

Primary references:
- [README.md](../../README.md)
- [docs/getting-started/README.md](../getting-started/README.md)
- [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- [docs/reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
- [docs/product/benchmark-one-pager-v1.md](benchmark-one-pager-v1.md)
- [docs/operations/release-upgrade-compat-checklist-v1.md](../operations/release-upgrade-compat-checklist-v1.md)

## Key value pillars in this release

### Deterministic replay
- append-only WAL remains the source of truth,
- recovery and replay workflows are part of the product model,
- historical inspection is built around replay-safe semantics rather than ad hoc reconstruction.

### Explicit boundaries
- domains remain application-visible boundaries,
- cross-domain work is explicit,
- transaction scope is part of the model, not hidden in application folklore.

### Replay-safe history and temporal inspection
- `AS OF LSN`, `FOR HISTORY`, and temporal helper functions remain first-class workflows,
- fixture-first and temporal debugging paths are surfaced in onboarding and Studio.

### pgwire-first adoption
- the main application-facing runtime remains pgwire,
- documented compatibility remains a pragmatic subset for mainstream client/tool flows,
- the release does not claim full PostgreSQL parity.

### Operator clarity
- release and production docs emphasize health, recovery, compatibility, lag, and failover visibility,
- ASQL Studio and documentation are aligned around temporal and cluster/operator workflows.

## What changed in the public product surface

This release candidate consolidates and sharpens the public evaluation surface around:
- the canonical `cmd/asqld` + pgwire path,
- ASQL Studio as the desktop workflow through `-pgwire-endpoint`,
- compatibility policy and compatibility matrix visibility,
- benchmark one-pager visibility for evaluators,
- launch/evaluation docs that clearly state what ASQL is and is not.

## Compatibility and claim boundaries

For `v1.0.0-rc1`, treat the compatibility surface as:
- a documented PostgreSQL-compatible subset over pgwire,
- suitable for the supported `pgx`, `psql`, and mainstream startup/metadata flows already covered in the compatibility docs,
- explicitly narrower than full PostgreSQL behavior.

Use these docs as the source of truth:
- [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- [docs/reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
- [docs/reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)

## Known limits / narrowed claims

This release candidate does **not** claim:
- drop-in PostgreSQL replacement behavior,
- broad PostgreSQL parity beyond the documented subset,
- workflow-engine semantics,
- vertical product semantics for finance, pharma, or manufacturing.

## Recommended evaluator checklist

Before adopting more deeply, verify:
- the getting-started pgwire flow works in your environment,
- your client/driver/tool falls inside the documented compatibility surface,
- your first domain boundaries are explicit,
- your first deterministic fixture pack works,
- your temporal debugging workflow uses `FOR HISTORY` and `AS OF LSN` early,
- your operational expectations match the current benchmark and production/readiness docs.

## Links

- Launch narrative: [docs/product/launch-narrative-v1.md](launch-narrative-v1.md)
- Technical post: [docs/product/technical-launch-post-v1.md](technical-launch-post-v1.md)
- Benchmark one-pager: [docs/product/benchmark-one-pager-v1.md](benchmark-one-pager-v1.md)
- Getting started: [docs/getting-started/README.md](../getting-started/README.md)
- Compatibility matrix: [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- Release/upgrade checklist: [docs/operations/release-upgrade-compat-checklist-v1.md](../operations/release-upgrade-compat-checklist-v1.md)
