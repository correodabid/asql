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
   dedicated end-to-end regression.

## End-to-end regression-covered behavior

| Situation | SQLSTATE | Evidence | Notes |
|---|---|---|---|
| Query cancellation after PostgreSQL `CancelRequest` | `57014` (`query_canceled`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable` | The connection remains usable after the canceled query. |
| Client-aborted `COPY FROM STDIN` via `CopyFail` | `57014` (`query_canceled`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCopyFailRollsBackInsertedRows` | Abort rolls back inserted rows and returns a cancellation-style error. |
| Invalid binary bind payload for the currently supported narrow binary subset | `22P02` (`invalid_text_representation`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryDiscardsMessagesUntilSyncAfterError` | Applies to malformed binary bind input in the current `int4` / `int8` / `bool` compatibility wedge. |

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
| Follower rejects a write and provides leader redirect hint | `25006` (`read_only_sql_transaction`) | Partial | Implemented in `sendFollowerRedirectError()` and `extendedFollowerRedirect()`. SDK-side redirect-hint parsing is covered in `sdk/cluster_test.go`, but the exact wire-level error is not yet asserted in a dedicated pgwire regression. |
| Password auth failure when `AuthToken` is configured | `28P01` (`invalid_password`) | Partial | Startup path implementation sends `28P01`; `TestPGWirePasswordAuthenticationWithAuthToken` proves the connection fails for missing/wrong password, but does not currently assert the exact SQLSTATE on the wire. |
| Protocol violation during password startup | `08P01` (`protocol_violation`) | Implementation-present | Startup path sends `08P01` when a password challenge is answered with the wrong frontend message type. No dedicated regression covers this exact path yet. |
| Bare PostgreSQL transaction syntax guardrail (`BEGIN`) | currently coarse, message-oriented | Partial | `TestPGWireCompatibilityUnsupportedPatternGuidance` asserts actionable guidance text, not a specific SQLSTATE. Treat the guidance message as the stable contract today, not an exact PostgreSQL parity claim. |
| `ANY(...)` / `ARRAY[...]` guardrail | currently coarse, message-oriented | Partial | `TestPGWireCompatibilityUnsupportedPatternGuidance` asserts actionable guidance text, not a specific SQLSTATE. |

## What clients should rely on today

Safe public compatibility claims today:

- `57014` for explicit pgwire cancellation paths already covered end to end,
- `22P02` for malformed binary bind input in the current supported binary-bind subset,
- the documented message-based mapper for common error families when treated as
  **current behavior**, not full PostgreSQL parity.

Client code should be more cautious when depending on:

- exact SQLSTATEs for auth-startup failure paths,
- exact SQLSTATEs for ASQL-native guardrail errors,
- exact SQLSTATEs for all planner/executor failures beyond the mapper's current
  coarse classification.

## Follow-up guidance

If a new compatibility claim depends on exact SQLSTATE behavior, do not publish
it until:

1. the path has a dedicated regression test,
2. this note and the evidence map are updated together,
3. the claim is reflected consistently in the compatibility surface docs.
