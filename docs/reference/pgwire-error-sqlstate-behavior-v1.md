# pgwire error and SQLSTATE behavior v1

## Purpose

This note documents ASQL's current pgwire error-response and SQLSTATE behavior.

It is intentionally narrower than full PostgreSQL error compatibility. Use it to
understand which SQLSTATEs are already stable enough to mention publicly, which
ones are implemented through ASQL's current message-based mapper, and which
areas should still be treated as coarse compatibility rather than parity.

Use it together with:

- [sql-pgwire-compatibility-policy-v1.md](sql-pgwire-compatibility-policy-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [postgres-compatibility-evidence-v1.md](postgres-compatibility-evidence-v1.md)
- [pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md)

## Stance

ASQL does not promise full PostgreSQL error-surface parity.

Current behavior falls into three buckets:

1. **End-to-end regression-covered** — public compatibility docs can rely on it.
2. **Unit-covered current mapping** — implemented and tested in the pgwire mapper,
   but still based on coarse message classification rather than rich typed engine
   errors.
3. **Implementation-present / partially evidenced** — behavior exists in code and
  may be exercised indirectly, but the exact SQLSTATE is not yet covered by a
  dedicated regression.

## End-to-end regression-covered behavior

| Situation | SQLSTATE | Evidence | Notes |
|---|---|---|---|
| Query cancellation after PostgreSQL `CancelRequest` | `57014` (`query_canceled`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable` | The connection remains usable after the canceled query. |
| Client-aborted `COPY FROM STDIN` via `CopyFail` | `57014` (`query_canceled`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCopyFailRollsBackInsertedRows` | Abort rolls back inserted rows and returns a cancellation-style error. |
| Invalid binary bind payload for the currently supported narrow binary subset | `22P02` (`invalid_text_representation`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryDiscardsMessagesUntilSyncAfterError` | Applies to malformed binary bind input in the current `int4` / `int8` / `bool` compatibility wedge. |
| Password auth failure when `AuthToken` is configured | `28P01` (`invalid_password`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWirePasswordAuthenticationWrongPasswordReturns28P01` | This is now covered at the raw pgwire startup/auth handshake level. |
| Wrong frontend message during password startup | `08P01` (`protocol_violation`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWirePasswordAuthenticationWrongMessageReturns08P01` | Covers the password-challenge path where the client sends a non-password frontend message. |
| Follower redirect error shape with leader hint | `25006` (`read_only_sql_transaction`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestSendFollowerRedirectErrorWrites25006AndHint` | Covers the wire-level redirect message and `asql_leader=...` hint shape used by SDK clients. |
| `COMMIT` without an active ASQL transaction | `25P01` (`no_active_sql_transaction`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireTransactionStateSQLStates/commit_without_active_transaction` | Covers the current ASQL-native transaction-state guardrail through pgwire. |
| Starting a second ASQL transaction while one is already active | `25001` (`active_sql_transaction`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireTransactionStateSQLStates/begin_while_transaction_already_active` | Covers the current single active transaction per session behavior. |
| Missing table in a pgwire query | `42P01` (`undefined_table`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates/undefined_table` | Covers a common planner/object failure end to end. |
| Duplicate table create on commit | `42P07` (`duplicate_table`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates/duplicate_table_on_commit` | The current mapper uses PostgreSQL's duplicate-table code generically for duplicate object creation. |
| Duplicate primary/unique key surfaced on commit | `23505` (`unique_violation`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates/unique_violation_on_commit` | Covers ASQL's current commit-time uniqueness enforcement shape. |
| `NOT NULL` violation surfaced on commit | `23502` (`not_null_violation`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates/not_null_violation_on_commit` | Covers ASQL's current commit-time nullability enforcement shape. |
| `CHECK` violation surfaced on commit | `23514` (`check_violation`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates/check_violation_on_commit` | Covers ASQL's current commit-time check-constraint enforcement shape. |

## Unit-covered current mapper behavior

ASQL currently maps engine errors to SQLSTATEs using substring classification in
[internal/server/pgwire/errors.go](../../internal/server/pgwire/errors.go).
The mapping is unit-covered in `internal/server/pgwire/errors_test.go`, but it
is still a coarse compatibility layer rather than a typed PostgreSQL-equivalent
error hierarchy.

| Error family | Current SQLSTATE | Mapper coverage |
|---|---|---|
| syntax / parse errors | `42601` | `TestSQLStateFromMessageMappings` |
| undefined function | `42883` | `TestSQLStateFromMessageMappings` |
| undefined column | `42703` | `TestSQLStateFromMessageMappings` |
| undefined table | `42P01` | `TestSQLStateFromMessageMappings` |
| duplicate object (`already exists`) | `42P07` | `TestSQLStateFromMessageMappings` |
| unique constraint violation | `23505` | `TestSQLStateFromMessageMappings`, `TestMapErrorToSQLState` |
| foreign key violation | `23503` | `TestSQLStateFromMessageMappings` |
| not-null violation | `23502` | `TestSQLStateFromMessageMappings` |
| check constraint violation | `23514` | `TestSQLStateFromMessageMappings` |
| no active transaction | `25P01` | `TestSQLStateFromMessageMappings` |
| already-in-transaction state | `25001` | `TestSQLStateFromMessageMappings` |
| invalid transaction state (`domain is required`) | `25000` | `TestSQLStateFromMessageMappings` |
| serialization / write conflict | `40001` | `TestSQLStateFromMessageMappings` |
| deadlock | `40P01` | `TestSQLStateFromMessageMappings` |
| invalid input / cast mismatch | `22P02` | `TestSQLStateFromMessageMappings` |
| numeric out of range | `22003` | `TestSQLStateFromMessageMappings` |
| division by zero | `22012` | `TestSQLStateFromMessageMappings` |
| generic missing object fallback | `42000` | `TestSQLStateFromMessageMappings` |
| uncategorized internal error fallback | `XX000` | `TestSQLStateFromMessageMappings` |

## Implementation-present / partially evidenced behavior

| Situation | Current SQLSTATE | Evidence status | Notes |
|---|---|---|---|
| Bare PostgreSQL transaction syntax guardrail (`BEGIN`) | currently coarse, message-oriented | Partial | `TestPGWireCompatibilityUnsupportedPatternGuidance` asserts actionable guidance text, not a specific SQLSTATE. Treat the guidance message as the stable contract today, not an exact PostgreSQL parity claim. |
| Empty or malformed ASQL transaction-open statements | currently coarse / not yet claimed | Partial | Keep the guidance/message surface as the contract until a dedicated regression proves a stable exact SQLSTATE for these malformed `BEGIN DOMAIN ...` cases. |
| `ANY(...)` / `ARRAY[...]` guardrail | currently coarse, message-oriented | Partial | `TestPGWireCompatibilityUnsupportedPatternGuidance` asserts actionable guidance text, not a specific SQLSTATE. |

## What clients should rely on today

Safe public compatibility claims today:

- `57014` for explicit pgwire cancellation paths already covered end to end,
- `22P02` for malformed binary bind input in the current supported binary-bind subset,
- the documented message-based mapper for common error families when treated as
  **current behavior**, not full PostgreSQL parity.

Client code should be more cautious when depending on:

- exact SQLSTATEs for planner/executor failure families not yet listed in the
  end-to-end table above,
- exact SQLSTATEs for ASQL guardrail paths that still only have guidance-style
  tests rather than dedicated SQLSTATE regressions,
- exact SQLSTATEs for broader PostgreSQL parity claims beyond the documented
  subset of end-to-end-covered paths.

## Follow-up guidance

If a new compatibility claim depends on exact SQLSTATE behavior, do not publish
it until:

1. the path has a dedicated regression test,
2. this note and the evidence map are updated together,
3. the claim is reflected consistently in the compatibility surface docs.
