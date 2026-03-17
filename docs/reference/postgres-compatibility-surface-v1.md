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

For claim-to-test traceability and current evidence gaps, see
[postgres-compatibility-evidence-v1.md](postgres-compatibility-evidence-v1.md).

For current pgwire error-response and SQLSTATE behavior, including where ASQL
matches PostgreSQL closely enough to claim compatibility today, see
[pgwire-error-sqlstate-behavior-v1.md](pgwire-error-sqlstate-behavior-v1.md).

## Supported (Spike v1)
- Protocol: PostgreSQL startup, simple query flow, and the extended query pipeline (`Parse` / `Bind` / `Describe` / `Execute` / `Sync`).
- SSL negotiation: `SSLRequest` is handled with `N` (no TLS). Clients using `sslmode=prefer` (the default for psql, pgx, JDBC, DBeaver, DataGrip, pgAdmin) fall back to plaintext successfully. `sslmode=disable` and `sslmode=allow` also work. `sslmode=require` / `verify-ca` / `verify-full` are unsupported and will fail.
- Authentication:
  - `AuthenticationOk` when no pgwire password is configured
  - `AuthenticationCleartextPassword` when `AuthToken` / `-auth-token` is configured or when the durable principal catalog is present
  - password validation can use either the shared deployment token or stored database principals, depending on whether the durable principal catalog has been bootstrapped
  - deployment/operator tokens and durable database principals are intentionally different layers: tokens protect transport/admin surfaces, while principals and grants govern pgwire identity and in-database authorization
  - current MVP privilege surface when the durable principal catalog is present: any authenticated enabled principal can execute current-state `SELECT`, `ADMIN` is required for current DDL/DML/schema-changing statements and operator/admin virtual-schema helpers under `asql_admin.*`, and `SELECT_HISTORY` is additionally required for temporal reads plus historical helper views such as `asql_admin.row_history` / `asql_admin.entity_version_history`
  - for historical reads, pgwire evaluates `SELECT_HISTORY` against the current durable principal/grant state even when the target data snapshot is older
- Session states: `ReadyForQuery` idle (`I`) and in-transaction (`T`).
- Session/setup compatibility shim:
  - `current_database()`, `version()`, `current_schema()`, `current_user`, `session_user`, and `user`
  - when a durable principal authenticates, `current_user` / `session_user` and `current_setting('session_authorization')` reflect that authenticated principal
  - `SHOW server_version`, `SHOW server_version_num`, `SHOW search_path`, plus generic `SHOW <param>` fallback for common tool probes
  - `SET`, `RESET`, `RESET ALL`, `DEALLOCATE`, and `DEALLOCATE ALL` are accepted as deterministic no-ops for client/tool session management
- Catalog/introspection compatibility shim:
  - `current_setting('...')`, `set_config(...)`, `pg_is_in_recovery()`, `pg_backend_pid()`, `inet_server_addr()`, `inet_server_port()`, `pg_encoding_to_char()`
  - `obj_description()`, `col_description()`, `shobj_description()` return empty results
  - `has_schema_privilege()`, `has_table_privilege()`, `has_database_privilege()` stay compatibility-oriented, but become principal-aware when the durable principal catalog is present
  - current grant-aware subset: `CREATE` follows `ADMIN`, `SELECT_HISTORY` follows the explicit temporal privilege, and common introspection privileges such as database `CONNECT`, schema `USAGE`, and mainstream table-operation probes reflect the current authenticated principal instead of always returning `true`
  - synthetic catalog coverage for `pg_catalog.pg_tables`, `pg_catalog.pg_namespace`, `pg_catalog.pg_class`, `pg_catalog.pg_attribute`, `pg_catalog.pg_type`, `pg_catalog.pg_settings`, `pg_catalog.pg_database`, `information_schema.tables`, `information_schema.columns`, and `information_schema.schemata`
  - a smaller set of PostgreSQL catalog tables (`pg_index`, `pg_constraint`, `pg_proc`, `pg_am`, `pg_extension`, `pg_roles`, `pg_authid`, `pg_user`) is intercepted with empty result sets to keep mainstream tool flows moving without claiming full parity
- Query mode:
  - simple query protocol
  - extended query protocol for the current ASQL SQL subset
- Extended-query behavior:
  - named prepared statements and portals are supported per connection
  - `Describe Statement` returns `ParameterDescription`; common schema-aligned `INSERT ... VALUES ($n, ...)`, `UPDATE ... SET col = $n`, arithmetic `UPDATE ... SET col = col OP $n`, and simple `WHERE column OP $n` predicate shapes can advertise concrete scalar OIDs instead of only unspecified (`0`) entries
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
| `SELECT ... WHERE <scalar predicate>` | Supported | Current ASQL SQL subset; with durable principals enabled, any authenticated enabled principal can execute current-state reads. |
| `ORDER BY ... LIMIT n` | Supported | Covered in executor and pgwire tests. |
| Literal `IN (...)` / `NOT IN (...)` | Supported | Covered in executor tests. |
| Subquery-based `IN (SELECT ...)` | Supported | Covered in executor tests for current shapes. |
| `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)` | Supported | Covered in parser and executor tests. |
| `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` | Supported | Covered in executor tests for current join shapes. |
| Simple `WITH` / CTE shapes | Supported | Covered in executor tests for current non-recursive shapes. |
| `ILIKE` / `NOT ILIKE` | Supported | Covered in executor tests. |
| `INSERT ... RETURNING ...` | Supported | Current `RETURNING` support is insert-focused; with durable principals enabled, current DML requires `ADMIN`. |
| `INSERT ... ON CONFLICT ...` | Supported | `DO NOTHING` and current `DO UPDATE` shapes are covered in executor tests. |
| `TRUNCATE TABLE ...` | Supported | Covered in parser and executor tests; with durable principals enabled, current schema/data-destructive mutations require `ADMIN`. |
| `DROP TABLE IF EXISTS` / `DROP INDEX IF EXISTS` | Supported | Covered in executor tests. |
| Extended protocol with scalar bind parameters | Supported | Session-scoped prepared statements/portals. |
| Parameterized predicates like `WHERE id >= $1` | Supported | Covered through pgwire regression tests. |
| Cross-domain transactions via `BEGIN CROSS DOMAIN ...` | Supported | ASQL-native transaction model. |
| Temporal helpers like `current_lsn()` / `row_lsn(...)` | Supported | ASQL-native surface over SQL/pgwire. |
| `AS OF LSN` / `AS OF TIMESTAMP` / `FOR HISTORY` with durable-principal authz | Supported | Requires explicit `SELECT_HISTORY`; authorization is evaluated against the current principal/grant state, not a historical grant snapshot. |
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
| `CancelRequest` / SQLSTATE `57014` cancel path | Implemented | Regression-covered | Conformance tests cover query cancellation plus post-cancel connection usability. |
| Narrow `COPY FROM STDIN` / `COPY TO STDOUT` flows | Implemented | Regression-covered | Conformance tests cover chunked copy-in, copy-out streaming, and `CopyFail` rollback behavior. |
| `EXISTS` / `NOT EXISTS` | Implemented | Regression-covered | Covered in parser and executor tests for current shapes. |
| `LEFT JOIN` / `RIGHT JOIN` / `CROSS JOIN` | Implemented | Regression-covered | Covered in executor tests for current supported join shapes. |
| Qualified `table.*` / `alias.*` projection | Implemented | Regression-covered | Current support expands to unqualified output columns for tested shapes, including derived-table workflows. |
| Non-correlated derived tables in `FROM` | Implemented | Regression-covered | Current support is limited to tested alias-required, non-`LATERAL`, non-correlated shapes. |
| Non-recursive `WITH` / CTE shapes | Implemented | Regression-covered | Current documented support should remain limited to tested shapes. |
| `ILIKE` / `NOT ILIKE` | Implemented | Regression-covered | Covered in executor tests. |
| `INSERT ... RETURNING` | Implemented | Regression-covered | This is the only `RETURNING` path currently documented as supported. |
| `INSERT ... ON CONFLICT ...` | Implemented | Regression-covered | Current `DO NOTHING` and supported `DO UPDATE` shapes are tested. |
| `TRUNCATE TABLE` | Implemented | Regression-covered | Covered in parser and executor tests. |
| `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` | Implemented | Regression-covered | Covered by parser tests plus dedicated pgwire end-to-end regression coverage for duplicate-safe create flows. |
| `UPDATE ... RETURNING` / `DELETE ... RETURNING` | Unsupported | Clear negative status | Do not assume PostgreSQL parity here because `INSERT ... RETURNING` works. |

Documentation rule going forward:

- `Implemented + regression-covered` can be claimed publicly in this matrix.
- `Implemented + partial evidence` should stay cautiously worded until
  dedicated end-to-end compatibility coverage exists.
- `Unsupported` should remain explicit to avoid adoption surprises.

## Intended Use
- Compatibility wedge for early client interoperability and migration experiments.
- Good enough for a subset of mainstream driver flows, but not a full drop-in PostgreSQL server replacement.
