# ASQL GitHub Release Description — v1.0.0-rc1

Date: 2026-03-17
Status: prepared GitHub release description draft.

## Suggested GitHub release title

`ASQL v1.0.0-rc1 — deterministic SQL engine release candidate`

## Release description

ASQL `v1.0.0-rc1` is the first release candidate for the deterministic SQL engine built in Go.

ASQL is designed for teams that need:
- explicit domain boundaries,
- replay-safe history,
- deterministic replay from append-only WAL,
- temporal inspection with `AS OF LSN` and `FOR HISTORY`,
- and a practical pgwire path from single node to clustered operation.

ASQL exposes a pragmatic PostgreSQL-compatible subset over pgwire for documented client and tool flows.
It is **not** a drop-in PostgreSQL replacement.

### Recommended evaluation path

1. Start the canonical server runtime:
   - `go run ./cmd/asqld -addr :5433 -data-dir .asql`
2. Connect through pgwire.
3. Validate one explicit domain-scoped workflow.
4. Try historical reads and fixtures early.
5. Review compatibility, benchmark, and production docs before widening assumptions.
6. Use ASQL Studio through `-pgwire-endpoint` for temporal and operator workflows.

### Start here

- Getting started: [docs/getting-started/README.md](../getting-started/README.md)
- Compatibility matrix: [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- Compatibility policy: [docs/reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
- Benchmark one-pager: [docs/product/benchmark-one-pager-v1.md](benchmark-one-pager-v1.md)
- Release / upgrade checklist: [docs/operations/release-upgrade-compat-checklist-v1.md](../operations/release-upgrade-compat-checklist-v1.md)
- Technical launch post: [docs/product/technical-launch-post-v1.md](technical-launch-post-v1.md)

### What this release candidate emphasizes

- trust before breadth,
- explicit compatibility and release-gate posture,
- canonical pgwire-first onboarding,
- operator visibility for temporal and cluster workflows,
- precise claims instead of parity overreach.

### Narrowed claims / known boundaries

This release candidate does **not** claim:
- full PostgreSQL parity,
- drop-in PostgreSQL replacement behavior,
- workflow-engine semantics,
- vertical product semantics.

### Evaluation guidance

Best-fit evaluators are teams that benefit from:
- explicit boundaries,
- historical debugging,
- deterministic replay,
- and an operational model they can reason about safely.

If your top requirement is broad PostgreSQL parity above all else, ASQL is probably not the right first choice.
