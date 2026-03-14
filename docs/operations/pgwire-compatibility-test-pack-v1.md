# pgwire compatibility test pack v1

## Purpose

This pack turns ASQL's public PostgreSQL-compatibility claims into a repeatable
release-validation bundle grouped by client or tool shape.

Use it together with:

- [../reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
- [../reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- [../reference/postgres-compatibility-evidence-v1.md](../reference/postgres-compatibility-evidence-v1.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)

## Release usage rule

For every release candidate, run the lanes below and record:

1. which lanes were executed,
2. which tests/files provided the evidence,
3. whether any failure belongs to docs, pgwire shim/protocol, or SQL surface,
4. whether public compatibility docs changed in the same release window.

If a lane regresses, do not broaden public compatibility claims until the lane
is green again or the docs are narrowed explicitly.

## Lanes

### Lane A — `pgx` simple protocol baseline

Goal: prove the lowest-surprise application path still works.

Primary evidence:

- `internal/server/pgwire/server_test.go`
  - `TestPGWireSimpleQueryRoundtrip`
  - `TestPGWireCompatibilitySupportedPatterns`
- `internal/server/pgwire/history_regression_test.go`
  - `TestPGWireForHistoryRegressionStableMetadataAndRows`

What this lane covers:

- connection startup,
- domain transaction flow,
- DDL + DML roundtrip,
- scalar bind parameters,
- `ORDER BY` / `LIMIT` / `OFFSET`,
- literal `IN (...)`,
- stable row metadata for pgx-facing result sets.

### Lane B — `pgx` / extended query protocol baseline

Goal: prove prepared statements, portals, bind semantics, and protocol
recovery stay inside the documented subset.

Primary evidence:

- `internal/server/pgwire/extended_query_conformance_test.go`
  - `TestExtendedQueryPortalResumesAcrossExecuteCalls`
  - `TestExtendedQueryDescribeStatementInfersParameterCount`
  - `TestExtendedQueryDiscardsMessagesUntilSyncAfterError`
  - `TestExtendedQueryBinaryBindSupportsInt4Int8AndBool`
  - `TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable`

What this lane covers:

- `Parse` / `Bind` / `Describe` / `Execute` / `Sync`,
- `ParameterDescription`,
- portal suspend/resume,
- error discard until `Sync`,
- narrow binary bind support (`int4`, `int8`, `bool`),
- cancel behavior on pgwire-managed execution boundaries.

### Lane C — `psql` baseline

Goal: prove the main psql startup, introspection, and session-management probes
still work against the documented compatibility wedge.

Primary evidence:

- `internal/server/pgwire/server_test.go`
  - `TestSSLModePreferFallback`
  - `TestCatalogStartupIntrospectionQueries`
  - `TestCatalogEmptyInterceptsExposeSchemaAcrossProtocols`
  - `TestPGWireCompatibilityUnsupportedPatternGuidance`

Important sub-areas to verify from those tests:

- `sslmode=prefer` fallback,
- `current_setting(...)`, `SHOW`, `current_database()`, `current_user`,
- `pg_database`, `pg_namespace`, `pg_settings`,
- session-management no-ops,
- explicit guardrail errors for unsupported PostgreSQL-shaped assumptions.

### Lane D — JDBC / GUI baseline

Goal: prove mainstream metadata-driven tools still get the startup and catalog
responses they need without claiming full PostgreSQL parity.

Primary evidence:

- `internal/server/pgwire/server_test.go`
  - `TestCatalogStartupIntrospectionQueries`
  - `TestCatalogEmptyInterceptsExposeSchemaAcrossProtocols`
  - `TestShowUnknownParamFallbackWorksOnExtendedProtocol`

Expected behaviors:

- `SET`, `set_config`, `version()`, `current_schema()`, `SHOW`,
- `pg_type`, `pg_settings`, `information_schema.schemata`,
- empty-result catalog intercepts still return usable schema metadata,
- non-`asql_*` `SHOW <param>` probes do not derail startup.

### Lane E — COPY and bulk-ingest baseline

Goal: prove the currently documented narrow `COPY` surface remains safe.

Primary evidence:

- `internal/server/pgwire/extended_query_conformance_test.go`
  - `TestCopyFromStdinInsertsRowsAndAcceptsChunkedCopyData`
  - `TestCopyToStdoutStreamsRows`
  - `TestCopyFromStdinCSVInsertsQuotedValues`
  - `TestCopyToStdoutCSVQuotesValues`
  - `TestCopyFailRollsBackInsertedRows`

What this lane covers:

- text-mode `COPY FROM STDIN` / `TO STDOUT`,
- CSV-mode `COPY FROM STDIN` / `TO STDOUT`,
- chunked `CopyData`,
- quoted CSV fields,
- rollback on `CopyFail`.

## Minimum release-candidate bundle

At RC time, treat these as the minimum compatibility evidence summary:

- Lane A green
- Lane B green
- one representative startup/catalog lane green (`psql` or JDBC/GUI), with the
  other lane checked whenever pgwire shim/catalog code changed
- Lane E green when `COPY` behavior or parser/shim code changed

## Triage rule for failures

When a lane fails, classify it before changing code:

1. **Docs-only issue** — the behavior is real, but docs over- or under-claim it.
2. **Protocol/shim issue** — startup, session, catalog, metadata, or pgwire
   protocol behavior regressed.
3. **SQL surface issue** — parser/planner/executor behavior regressed for a
   documented SQL pattern.
4. **Out-of-scope issue** — the failing client/tool expects PostgreSQL parity
   outside ASQL's documented subset.

Only widen public compatibility claims when the relevant lane is green and the
matrix/evidence docs are updated in the same change window.

## Decision rubric for reported compatibility gaps

Use this rubric before accepting any new PostgreSQL-compatibility work:

### 1. Is the behavior already present?

- If **yes**, prefer documenting it and adding regression coverage before
  changing implementation.
- If **partially**, tighten docs first so they describe the real supported
  subset precisely.
- If **no**, continue to the next questions.

### 2. Does the gap affect a mainstream client/tool flow?

Treat these as high-signal inputs:

- `pgx` application paths,
- `psql` startup or normal operator flows,
- mainstream JDBC/GUI startup or schema-browsing flows,
- release-candidate pack lanes already documented here.

If the issue only appears in a broad ORM/parity-heavy path, default to caution
and avoid expanding surface area unless adoption evidence is strong.

### 3. Which layer owns the fix?

Choose exactly one primary bucket first:

- **Docs** — the feature works, but the docs are wrong, vague, or incomplete.
- **Protocol/catalog shim** — startup packets, session shims, `SHOW`,
  `current_setting`, synthetic catalog tables, metadata, or pgwire message
  behavior need adjustment.
- **SQL surface** — parser, planner, executor, or SQLSTATE behavior for a
  documented SQL shape needs work.
- **Reject as out of scope** — the request implies broad PostgreSQL parity,
  TLS/auth/catalog breadth, ORM assumptions, or syntax/semantics beyond the
  ASQL wedge.

Do not mix buckets prematurely. Fix the smallest owning layer first.

### 4. Does the change preserve ASQL constraints?

Do not take the change if it would weaken:

- determinism,
- ASQL-native transaction semantics,
- explicit domain boundaries,
- minimal product surface area.

If a compatibility request conflicts with those constraints, narrow the public
claim or reject the request explicitly.

### 5. What is required before the claim becomes public?

Every accepted compatibility expansion must land as one bundle:

1. implementation or doc correction,
2. regression tests,
3. compatibility matrix/evidence updates,
4. release-pack lane impact reviewed.

If those four pieces are not present together, the claim is not release-ready.

## Publication rule for new compatibility claims

No new PostgreSQL-compatibility claim is public until all of the following are
true in the same change window:

1. the supported behavior is implemented or precisely documented,
2. at least one regression/conformance test covers the claim,
3. [../reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
   reflects the supported boundary,
4. [../reference/postgres-compatibility-evidence-v1.md](../reference/postgres-compatibility-evidence-v1.md)
   links the claim to its tests,
5. this test pack is updated if the new claim changes release-validation lanes.