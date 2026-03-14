# SQL and pgwire compatibility policy v1

## Stance

ASQL exposes a pragmatic PostgreSQL-compatible subset over pgwire.

This is a deliberate compatibility wedge for common application and tooling
flows, not a claim of broad PostgreSQL behavioral parity.

## What ASQL promises

- Stable support for a documented subset of pgwire startup and query flows.
- Stable support for ASQL transaction primitives over SQL:
  - `BEGIN DOMAIN <name>`
  - `BEGIN CROSS DOMAIN <a>, <b>, ...`
  - `COMMIT`
  - `ROLLBACK`
- Stable support for the documented ASQL SQL subset used by app backends:
  simple predicates, joins within the current engine surface, ordering, `LIMIT`,
  deterministic temporal helpers, and versioned-reference workflows.
- Explicit documentation of unsupported and planned behaviors.

## What ASQL does not promise

- Drop-in PostgreSQL replacement semantics.
- Full PostgreSQL catalog, auth, TLS, type, planner, or SQL-feature parity.
- Broad compatibility with SQL patterns outside the documented ASQL subset.

## Contract shape

- The policy lives here.
- The precise supported/unsupported surface lives in
  [docs/reference/postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md).
- The claim-to-test evidence pack lives in
  [docs/reference/postgres-compatibility-evidence-v1.md](postgres-compatibility-evidence-v1.md).
- The operating model, triage rubric, and release pack live in
  [docs/operations/pgwire-compatibility-test-pack-v1.md](../operations/pgwire-compatibility-test-pack-v1.md).
- Practical client guidance lives in
  [docs/reference/pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md).
- Public compatibility claims must be backed by regression tests.
- New compatibility claims are not public until docs, tests, matrix entries,
  and evidence links land together.

## Intended usage

Choose ASQL's pgwire path when you want mainstream client interoperability for
documented workflows and are willing to stay inside the explicit ASQL subset.

If a workflow is not documented as supported, treat it as unsupported until the
matrix and tests say otherwise.