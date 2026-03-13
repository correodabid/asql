# PostgreSQL Compatibility Surface Matrix v1

## Scope
This document defines the explicit supported and unsupported surface of ASQL's
PostgreSQL-oriented SQL and pgwire compatibility layer.

Policy stance: ASQL supports a pragmatic PostgreSQL-compatible subset for
documented workflows. It is not a drop-in PostgreSQL replacement.

Audit note (2026-03-13): this matrix has been refreshed against current code
and regression tests so it better distinguishes the supported compatibility
subset from broader PostgreSQL parity.

For practical client recommendations by driver and query mode, see
[pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md).

## Supported (Spike v1)
- Protocol: PostgreSQL startup, simple query flow, and the extended query pipeline (`Parse` / `Bind` / `Describe` / `Execute` / `Sync`).
- SSL negotiation: `SSLRequest` is handled with `N` (no TLS). Clients using `sslmode=prefer` (the default for psql, pgx, JDBC, DBeaver, DataGrip, pgAdmin) fall back to plaintext successfully. `sslmode=disable` and `sslmode=allow` also work. `sslmode=require` / `verify-ca` / `verify-full` are unsupported and will fail.
- Authentication:
  - `AuthenticationOk` when no pgwire password is configured
  - `AuthenticationCleartextPassword` when `AuthToken` / `-auth-token` is configured
  - password validation is single-token and connection-scoped (not role-based)
- Session states: `ReadyForQuery` idle (`I`) and in-transaction (`T`).
- Session/setup compatibility shim:
  - `current_database()`, `version()`, `current_schema()`, `current_user`, and `user`
  - `SHOW server_version`, `SHOW server_version_num`, `SHOW search_path`, plus generic `SHOW <param>` fallback for common tool probes
  - `SET`, `RESET`, `RESET ALL`, `DEALLOCATE`, and `DEALLOCATE ALL` are accepted as deterministic no-ops for client/tool session management
- Catalog/introspection compatibility shim:
  - `current_setting('...')`, `set_config(...)`, `pg_is_in_recovery()`, `pg_backend_pid()`, `inet_server_addr()`, `inet_server_port()`, `pg_encoding_to_char()`
  - `obj_description()`, `col_description()`, `shobj_description()` return empty results
  - `has_schema_privilege()`, `has_table_privilege()`, `has_database_privilege()` return `true`
  - synthetic catalog coverage for `pg_catalog.pg_tables`, `pg_catalog.pg_namespace`, `pg_catalog.pg_class`, `pg_catalog.pg_attribute`, `pg_catalog.pg_type`, `pg_catalog.pg_settings`, `pg_catalog.pg_database`, `information_schema.tables`, `information_schema.columns`, and `information_schema.schemata`
  - a smaller set of PostgreSQL catalog tables (`pg_index`, `pg_constraint`, `pg_proc`, `pg_am`, `pg_extension`, `pg_roles`, `pg_authid`, `pg_user`) is intercepted with empty result sets to keep mainstream tool flows moving without claiming full parity
- Query mode:
  - simple query protocol
  - extended query protocol for the current ASQL SQL subset
- Extended-query behavior:
  - named prepared statements and portals are supported per connection
  - `Describe Statement` returns `ParameterDescription`
  - portals can suspend and resume across repeated `Execute` calls
  - post-error message discard semantics are aligned with `Sync` recovery behavior
  - parameters are inlined at bind time before ASQL execution
- Cancellation behavior:
  - PostgreSQL `CancelRequest` packets are supported
  - cancellation is best-effort and currently guaranteed for pgwire-managed execution/streaming boundaries
  - cancelled operations return SQLSTATE `57014` (`query_canceled`)
- `COPY` support:
  - `COPY domain.table (col, ...) FROM STDIN` is supported in text and CSV modes
  - `COPY domain.table (col, ...) TO STDOUT` is supported in text and CSV modes
  - explicit column lists are required for `COPY FROM STDIN`
  - backend `CopyData` chunking and `CopyFail` rollback are covered by conformance-style tests
- Statement subset:
  - `BEGIN DOMAIN <name>`
  - `BEGIN CROSS DOMAIN <a>, <b>`
  - `COMMIT`
  - `ROLLBACK`
  - `SAVEPOINT <name>` / `ROLLBACK TO [SAVEPOINT] <name>`
  - `CREATE TABLE`, `CREATE INDEX`
  - `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE TABLE`
  - `SELECT` (within current ASQL SQL subset)
- Row encoding: text format (`Format=0`) with deterministic row order driven by ASQL planner/executor semantics.
- Parameter formats:
  - text parameters supported
  - binary bind parameters supported for narrow scalar cases currently decoded by the server (`int4`/`int8`/`bool`)
- Type OIDs (current):
  - integers mapped as `INT8` (`OID 20`)
  - strings/default mapped as `TEXT` (`OID 25`)
- Client/tool validation:
  - `pgx/v5` roundtrip is validated in integration-like tests
  - raw pgwire conformance-style tests cover portal resume, parameter inference, and extended-protocol error recovery

## Common app workflow SQL matrix

| Pattern | Status | Notes |
|---|---|---|
| `SELECT ... WHERE <scalar predicate>` | Supported | Current ASQL SQL subset. |
| `ORDER BY ... LIMIT n` | Supported | Covered in executor and pgwire tests. |
| Literal `IN (...)` / `NOT IN (...)` | Supported | Covered in executor tests. |
| Subquery-based `IN (SELECT ...)` | Supported | Covered in executor tests for current shapes. |
| `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)` | Supported | Covered in parser and executor tests. |
| `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` | Supported | Covered in executor tests for current join shapes. |
| Simple `WITH` / CTE shapes | Supported | Covered in executor tests for current non-recursive shapes. |
| `LIKE` / `ILIKE` / `NOT ILIKE` | Supported | Covered in executor tests. |
| `INSERT ... RETURNING ...` | Supported | Current `RETURNING` support is insert-focused. |
| `INSERT ... ON CONFLICT ...` | Supported | `DO NOTHING` and current `DO UPDATE` shapes are covered in executor tests. |
| `TRUNCATE TABLE ...` | Supported | Covered in parser and executor tests. |
| `DROP TABLE IF EXISTS` / `DROP INDEX IF EXISTS` | Supported | Covered in executor tests. |
| Extended protocol with scalar bind parameters | Supported | Session-scoped prepared statements/portals. |
| Parameterized predicates like `WHERE id >= $1` | Supported | Covered through pgwire regression tests. |
| Cross-domain transactions via `BEGIN CROSS DOMAIN ...` | Supported | ASQL-native transaction model. |
| Temporal helpers like `current_lsn()` / `row_lsn(...)` | Supported | ASQL-native surface over SQL/pgwire. |
| `LIMIT ... OFFSET ...` pagination | Supported | Supported in the current SQL subset; keyset pagination is still recommended for large scans. |
| `UPDATE ... RETURNING` / `DELETE ... RETURNING` | Unsupported | `RETURNING` is not yet documented/supported end-to-end beyond `INSERT`. |
| Arrays / `ANY(...)` | Unsupported | Not part of the current ASQL subset. |
| Bare `BEGIN` / `START TRANSACTION` | Unsupported | Use `BEGIN DOMAIN ...` or `BEGIN CROSS DOMAIN ...`; guardrail errors are explicit. |
| Full PostgreSQL catalog parity | Unsupported | Only the documented shim subset above is supported. |
| Drop-in PostgreSQL transaction syntax/semantics | Unsupported | Use ASQL transaction primitives. |
| Broader PostgreSQL feature parity beyond documented subset | Planned/Unsupported | Add only with docs + regression tests. |

## Unsupported (v1)
- PostgreSQL password authentication methods beyond the narrow cleartext-password token flow above (MD5/SCRAM), role/user management.
- TLS transport for pgwire connections (assessed and deferred — current `SSLRequest -> N` is sufficient for all mainstream tools in default configuration; see TLS reassessment below).
- Full PostgreSQL type system and general binary formats/results.
- Broad PostgreSQL catalog/system-table compatibility beyond the documented shim subset above.
- PostgreSQL-specific SQL features outside ASQL current grammar and planner support.
- General PostgreSQL `COPY` compatibility beyond the narrow table-oriented `FROM STDIN` / `TO STDOUT` flow above (for example program/file targets, binary format, and option parity).
- Full server compatibility for PostgreSQL prepared-statement semantics beyond the currently supported session-scoped extended protocol path.
- Transaction commands beyond ASQL deterministic transaction model.
- PostgreSQL array literals and `ANY(...)` predicates.

## Guardrails for likely PostgreSQL assumptions

ASQL returns explicit actionable errors for several common PostgreSQL-shaped
patterns that are outside the supported subset.

- `BEGIN` / `START TRANSACTION` → points callers to `BEGIN DOMAIN ...` or
  `BEGIN CROSS DOMAIN ...`.
- `ANY(...)` / `ARRAY[...]` → points callers to `IN (...)`, `IN (SELECT ...)`,
  or JSON/row modeling alternatives.

## Determinism Notes
- Query execution remains delegated to ASQL engine primitives.
- Deterministic behavior is protected with repeated-roundtrip compatibility tests.
- Narrow `COPY` flows are validated with raw pgwire conformance tests, including chunked `CopyData` ingestion and rollback-on-`CopyFail` behavior.
- Protocol layer does not introduce wall-clock/randomness dependencies in query planning/execution.

## TLS Reassessment
Reassessed whether minimal TLS negotiation/auth surface is needed beyond `SSLRequest -> N`.

**Conclusion: No changes needed for tool interoperability.**

- All mainstream PostgreSQL tools default to `sslmode=prefer`, which sends `SSLRequest`, receives `N`, and falls back to plaintext.
- Tested modes: `sslmode=prefer` ✓, `sslmode=allow` ✓, `sslmode=disable` ✓.
- `sslmode=require` / `verify-ca` / `verify-full` will fail, which is the correct behavior when the server doesn't offer TLS.
- TLS transport for pgwire is a production-hardening feature (Phase 6+), not a tool-compatibility requirement.
- The gRPC cluster sidecar config already has `TLSCertPath`/`TLSKeyPath`/`TLSClientCAPath` fields for future use.

## Validated Tool Flows
The following mainstream PostgreSQL client/tool startup flows are validated with integration tests (`TestMainstreamToolStartupFlows`):

1. **psql**: `current_setting()`, `pg_encoding_to_char()`, `current_database()`, `current_user`, `SHOW`, `obj_description()`, `pg_namespace`, `pg_database`.
2. **DBeaver / DataGrip**: `SET`, `set_config()`, `pg_is_in_recovery()`, `version()`, `current_schema()`, `pg_type`, `pg_settings`, `information_schema.schemata`, `has_database_privilege()`.
3. **pgx (Go SDK)**: `current_setting()` for 5+ GUCs, `pg_backend_pid()`, `inet_server_addr()`, `inet_server_port()`, plus end-to-end simple-protocol data workflows.

All three flows complete without errors and are followed by an end-to-end data workflow (CREATE TABLE, INSERT, UPDATE, DELETE, SELECT with WHERE/ORDER BY).

## Audit snapshot: selected feature status

This section is intentionally narrower than the full matrix. It exists to make
current documentation drift visible and to separate `implemented and
regression-covered` behaviors from items that are only partially evidenced.

| Feature | Status | Evidence status | Notes |
|---|---|---|---|
| Session/setup shim (`current_database`, `version`, `current_schema`, `current_user`, `SHOW`, `SET`/`RESET`/`DEALLOCATE`) | Implemented | Regression-covered | Exercised by mainstream tool startup tests. |
| Synthetic catalog subset (`pg_tables`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_settings`, `pg_database`, `information_schema.*`) | Implemented | Regression-covered | Supported only as a targeted compatibility shim, not as catalog parity. |
| `EXISTS` / `NOT EXISTS` | Implemented | Regression-covered | Covered in parser and executor tests for current shapes. |
| `LEFT JOIN` / `RIGHT JOIN` / `CROSS JOIN` | Implemented | Regression-covered | Covered in executor tests for current supported join shapes. |
| Non-recursive `WITH` / CTE shapes | Implemented | Regression-covered | Current documented support should remain limited to tested shapes. |
| `ILIKE` / `NOT ILIKE` | Implemented | Regression-covered | Covered in executor tests. |
| `INSERT ... RETURNING` | Implemented | Regression-covered | This is the only `RETURNING` path currently documented as supported. |
| `INSERT ... ON CONFLICT ...` | Implemented | Regression-covered | Current `DO NOTHING` and supported `DO UPDATE` shapes are tested. |
| `TRUNCATE TABLE` | Implemented | Regression-covered | Covered in parser and executor tests. |
| `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` | Implemented | Partial evidence | Present in parser/planner/executor/WAL paths, but not yet called out as a public compatibility claim with dedicated end-to-end regression coverage. |
| `UPDATE ... RETURNING` / `DELETE ... RETURNING` | Unsupported | Clear negative status | Do not assume PostgreSQL parity here because `INSERT ... RETURNING` works. |

Documentation rule going forward:

- `Implemented + regression-covered` can be claimed publicly in this matrix.
- `Implemented + partial evidence` should stay cautiously worded until
  dedicated end-to-end compatibility coverage exists.
- `Unsupported` should remain explicit to avoid adoption surprises.

## Intended Use
- Compatibility wedge for early client interoperability and migration experiments.
- Good enough for a subset of mainstream driver flows, but not a full drop-in PostgreSQL server replacement.
