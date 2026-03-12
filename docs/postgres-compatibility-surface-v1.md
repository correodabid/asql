# PostgreSQL Compatibility Surface Matrix v1 (Spike)

## Scope
This document defines the explicit supported and unsupported surface of ASQL's PostgreSQL protocol compatibility spike (Sprint V3).

## Supported (Spike v1)
- Protocol: PostgreSQL startup, simple query flow, and the extended query pipeline (`Parse` / `Bind` / `Describe` / `Execute` / `Sync`).
- SSL negotiation: `SSLRequest` is handled with `N` (no TLS). Clients using `sslmode=prefer` (the default for psql, pgx, JDBC, DBeaver, DataGrip, pgAdmin) fall back to plaintext successfully. `sslmode=disable` and `sslmode=allow` also work. `sslmode=require` / `verify-ca` / `verify-full` are unsupported and will fail.
- Authentication:
  - `AuthenticationOk` when no pgwire password is configured
  - `AuthenticationCleartextPassword` when `AuthToken` / `-auth-token` is configured
  - password validation is single-token and connection-scoped (not role-based)
- Session states: `ReadyForQuery` idle (`I`) and in-transaction (`T`).
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
  - `INSERT`, `UPDATE`, `DELETE`
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

## Unsupported (Spike v1)
- PostgreSQL password authentication methods beyond the narrow cleartext-password token flow above (MD5/SCRAM), role/user management.
- TLS transport for pgwire connections (assessed and deferred — current `SSLRequest -> N` is sufficient for all mainstream tools in default configuration; see TLS reassessment below).
- Full PostgreSQL type system and general binary formats/results.
- PostgreSQL catalog/system-table compatibility (`pg_catalog`, `information_schema`).
  - **Partial support added**: `current_setting('param')` (22 GUCs), `set_config()`, `pg_is_in_recovery()`, `pg_backend_pid()`, `inet_server_addr/port()`, `pg_encoding_to_char()`, `obj/col/shobj_description()` (empty), privilege-check functions (always true), `pg_catalog.pg_settings`, `pg_catalog.pg_database`, plus `SHOW` for 15+ params. Simple-query path now routes through catalog interception.
- PostgreSQL-specific SQL features outside ASQL current grammar and planner support.
- General PostgreSQL `COPY` compatibility beyond the narrow table-oriented `FROM STDIN` / `TO STDOUT` flow above (for example program/file targets, binary format, and option parity).
- Full server compatibility for PostgreSQL prepared-statement semantics beyond the currently supported session-scoped extended protocol path.
- Transaction commands beyond ASQL deterministic transaction model.

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
2. **DBeaver / DataGrip**: `SET`, `set_config()`, `pg_is_in_recovery()`, `version()`, `pg_type`, `pg_settings`, `information_schema.schemata`, `has_database_privilege()`.
3. **pgx (Go SDK)**: `current_setting()` for 5+ GUCs, `pg_backend_pid()`, `inet_server_addr()`, `inet_server_port()`.

All three flows complete without errors and are followed by an end-to-end data workflow (CREATE TABLE, INSERT, UPDATE, DELETE, SELECT with WHERE/ORDER BY).

## Intended Use
- Compatibility wedge for early client interoperability and migration experiments.
- Good enough for a subset of mainstream driver flows, but not a full drop-in PostgreSQL server replacement.
