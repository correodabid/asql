# PostgreSQL Compatibility Evidence Map v1

## Purpose

This note ties ASQL's public PostgreSQL-compatibility claims to concrete test
coverage.

Use it together with:

- [sql-pgwire-compatibility-policy-v1.md](sql-pgwire-compatibility-policy-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [pgwire-error-sqlstate-behavior-v1.md](pgwire-error-sqlstate-behavior-v1.md)
- [pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md)
- [../operations/pgwire-compatibility-test-pack-v1.md](../operations/pgwire-compatibility-test-pack-v1.md)

Status meanings:

- **Direct**: explicit regression or conformance coverage exists today.
- **Partial**: the claim is substantially exercised, but some sub-parts are only
  indirectly covered.
- **Gap**: the public doc claims more than the current direct regression pack
  proves.

## pgwire and startup/session/catalog evidence

| Claim family | Evidence | Status | Notes |
|---|---|---|---|
| Startup + simple-query pgwire roundtrip | `internal/server/pgwire/server_test.go`: `TestPGWireSimpleQueryRoundtrip` | Direct | Covers connection startup, pgx simple protocol, domain transaction, DDL, DML, and ordered query roundtrip. |
| SSL negotiation fallback (`sslmode=prefer` / `allow` / `disable`) | `internal/server/pgwire/server_test.go`: `TestSSLModePreferFallback` | Direct | Confirms `SSLRequest -> N` fallback works for the documented plaintext-compatible modes. |
| Cleartext password auth for shared token and durable principals | `internal/server/pgwire/server_test.go`: `TestPGWirePasswordAuthenticationWithAuthToken`, `TestPGWirePasswordAuthenticationWithDurablePrincipal`, `TestPGWirePasswordAuthenticationRespectsPasswordRotation`, `TestPGWirePasswordAuthenticationRespectsDisableAndEnable`, `TestPGWirePasswordAuthenticationFailsAfterPrincipalDeletion`; `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWirePasswordAuthenticationWrongPasswordReturns28P01`, `TestPGWirePasswordAuthenticationWrongMessageReturns08P01` | Direct | Covers shared-token auth plus stored-principal auth, password rotation, disable/enable, deletion, and exact wire-level SQLSTATEs for wrong-password (`28P01`) and wrong-message protocol violation (`08P01`). |
| Historical-read authorization uses the current durable principal/grant state | `internal/server/pgwire/server_test.go`: `TestPGWireHistoricalReadRequiresSelectHistoryPrivilege`, `TestPGWireAuditEventsCoverLoginAndHistoricalReadAuthorization` | Direct | Covers denied and allowed historical reads over pgwire plus the audit trail that records both the requested historical target and the current `SELECT_HISTORY`-based grant context used for the authorization decision. |
| Session/setup compatibility shim (`current_database()`, `current_user`, `session_user`, `SHOW`, `version()`, `SET`) | `internal/server/pgwire/server_test.go`: `TestCatalogStartupIntrospectionQueries`, `TestShowUnknownParamFallbackWorksOnExtendedProtocol`, `TestMainstreamToolStartupFlows/psql_startup`, `TestMainstreamToolStartupFlows/dbeaver_datagrip_startup`, `TestPGWireSessionIdentityReflectsAuthenticatedPrincipal` | Direct | Includes explicit coverage for generic non-`asql_*` `SHOW <param>` fallback, session no-op probes (`RESET`, `RESET ALL`, `DEALLOCATE`, `DEALLOCATE ALL`), and dynamic session identity surfaces for durable principals. |
| Catalog/introspection shim (`current_setting`, `pg_is_in_recovery`, `pg_backend_pid`, `pg_database`, `pg_type`, `pg_settings`, `pg_namespace`, grant-aware privilege probes) | `internal/server/pgwire/server_test.go`: `TestCatalogStartupIntrospectionQueries`, `TestCatalogEmptyInterceptsExposeSchemaAcrossProtocols`, `TestMainstreamToolStartupFlows/psql_startup`, `TestMainstreamToolStartupFlows/dbeaver_datagrip_startup`, `TestMainstreamToolStartupFlows/pgx_go_driver_startup`, `TestPGWirePrivilegeProbesReflectDurablePrincipalGrants` | Direct | Mainstream introspection paths are covered, including schema-stable empty-result intercepts for `pg_index`, `pg_constraint`, `pg_proc`, `pg_am`, `pg_extension`, `pg_roles`, `pg_authid`, and `pg_user`, plus principal-aware `has_*_privilege(...)` behavior for the currently documented durable-principal privilege surface. |
| Follower write rejection returns redirect-oriented SQLSTATE/hint | `internal/server/pgwire/sqlstate_regression_test.go`: `TestSendFollowerRedirectErrorWrites25006AndHint`; `sdk/cluster_test.go`: `TestParseRedirectHint` | Direct | Covers the wire-level `25006` response shape plus SDK parsing of the `asql_leader=...` hint. |
| Common ASQL transaction-state SQLSTATEs over pgwire (`25P01`, `25001`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireTransactionStateSQLStates` | Direct | Covers `COMMIT` without an active transaction and attempting to start a second transaction in the same session. |
| Common planner/object/constraint SQLSTATEs over pgwire (`42P01`, `42P07`, `23505`, `23502`, `23514`) | `internal/server/pgwire/sqlstate_regression_test.go`: `TestPGWireObjectAndConstraintSQLStates` | Direct | Covers missing-table, duplicate-table, unique, not-null, and check-constraint failures end to end through pgwire. |
| Extended query pipeline (`Parse` / `Bind` / `Describe` / `Execute` / `Sync`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryPortalResumesAcrossExecuteCalls`, `TestExtendedQueryDescribeStatementInfersParameterCount`, `TestExtendedQueryDescribeStatementInfersInsertParameterOIDs`, `TestExtendedQueryDescribeStatementInfersPredicateParameterOIDs`, `TestExtendedQueryDescribeStatementInfersUpdateParameterOIDs`, `TestExtendedQueryDescribeStatementInfersArithmeticUpdateParameterOIDs`, `TestExtendedQueryDiscardsMessagesUntilSyncAfterError` | Direct | Covers prepared statements, portals, `ParameterDescription`, common schema-aware insert, update-assignment, arithmetic-update, and simple predicate parameter OID inference, suspend/resume behavior, and `Sync` recovery semantics. |
| Narrow binary bind parameter decoding (`int4`, `int8`, `bool`) | `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryBinaryBindSupportsInt4Int8AndBool` | Direct | Covers the currently documented binary bind subset, including signed `int4`, large `int8`, and boolean parameters. |
| CancelRequest + SQLSTATE `57014` + connection remains usable | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable` | Direct | Matches the documented best-effort cancellation claim at pgwire-managed execution boundaries. |
| `COPY FROM STDIN` / `COPY TO STDOUT`, chunked `CopyData`, `CopyFail` rollback | `internal/server/pgwire/extended_query_conformance_test.go`: `TestCopyFromStdinInsertsRowsAndAcceptsChunkedCopyData`, `TestCopyToStdoutStreamsRows`, `TestCopyFromStdinCSVInsertsQuotedValues`, `TestCopyToStdoutCSVQuotesValues`, `TestCopyFailRollsBackInsertedRows` | Direct | Covers text and CSV copy flows, including quoted CSV fields, chunked input, streaming output, and rollback on `CopyFail`. |
| `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` over pgwire | `internal/server/pgwire/server_test.go`: `TestPGWireCreateIfNotExistsRegression` | Direct | Confirms duplicate create fails without the guard and succeeds with `IF NOT EXISTS`. |
| `pgx` client roundtrip and metadata stability | `internal/server/pgwire/history_regression_test.go`: `TestPGWireForHistoryRegressionStableMetadataAndRows` | Direct | Covers stable column names and OIDs for a pgwire `FOR HISTORY` workflow. |

## Common SQL subset evidence

| Public pattern | Evidence | Status | Notes |
|---|---|---|---|
| Parameterized predicates over pgwire (`WHERE id >= $1`) | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilitySupportedPatterns` | Direct | Exercises bind parameters in a mainstream pgx query flow. |
| `ORDER BY ... LIMIT n` | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilitySupportedPatterns`; `internal/engine/executor/engine_test.go`: `TestTimeTravelQueryAppliesOrderByAndLimit`, `TestTimeTravelQueryAppliesMultiColumnOrderBy` | Direct | Good pgwire plus engine coverage. |
| `LIMIT ... OFFSET ...` | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilitySupportedPatterns`; `internal/engine/executor/engine_test.go`: `TestTimeTravelQueryAppliesLimitAndOffset` | Direct | Covered in both pgwire and engine regression tests, including large-offset empty results. |
| Literal `IN (...)` | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilitySupportedPatterns` | Direct | Covered in pgwire integration flow. |
| Subquery `IN (SELECT ...)` / `NOT IN (SELECT ...)` | `internal/engine/executor/engine_test.go`: `TestSubqueryIN`, `TestSubqueryNOTIN`, `TestSubqueryINWithAND`, `TestSubqueryINEmpty` | Direct | Engine-level evidence exists for current supported shapes. |
| `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)` | `internal/engine/executor/engine_test.go`: `TestSubqueryEXISTS`, `TestSubqueryNOTEXISTS` | Direct | Engine-level evidence exists for current supported shapes. |
| `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` | `internal/engine/executor/engine_test.go`: `TestTimeTravelQueryAppliesInnerJoin`, `TestLeftJoinWithUnmatched`, `TestRightJoinWithUnmatched`, `TestCrossJoinCardinality`, `TestLeftJoinAllMatched` | Direct | Join-family coverage exists in executor tests. |
| Qualified `table.*` / `alias.*` projection | `internal/engine/executor/engine_test.go`: `TestQualifiedStarSingleTableAlias`, `TestQualifiedStarInsideDerivedTable` | Direct | Covers current qualified-star expansion for single-table alias queries and derived-table window workflows. |
| Non-correlated derived tables in `FROM` | `internal/engine/parser/parser_test.go`: `TestParseDerivedTableInFrom`, `TestParseDerivedTableJoin`, `TestParseJoinWithDerivedRightTable`, `TestParseMultipleDerivedTableJoins`; `internal/engine/executor/engine_test.go`: `TestDerivedTableWithWindowJoin`, `TestMultipleDerivedTableJoins`, `TestBaseTableWithMultipleDerivedJoins`, `TestFullyQualifiedBaseTableWithMultipleDerivedJoins`, `TestAliasedQualifiedColumnsAcrossMultipleDerivedJoins` | Direct | Covers current alias-required derived-table shapes, chained derived joins, domain-qualified join predicates, and aliased qualified-column projections across joined derived sources. |
| Simple non-recursive CTEs | `internal/engine/executor/engine_test.go`: `TestCTEBasic`, `TestCTEWithMainWhere`, `TestCTEMultiple` | Direct | Supports the current non-recursive claim. |
| `ILIKE` / `NOT ILIKE` | `internal/engine/executor/engine_test.go`: `TestILikeOperator` | Direct | Covers case-insensitive matching plus the negative form for the currently documented subset. |
| Extended-query `RowDescription` for qualified `alias.*` and derived-table `SELECT *` shapes | `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryQualifiedStarUsesSchemaAwareRowDescription`, `TestExtendedQueryDerivedTableRowDescriptionFollowsExpandedColumns`, `TestExtendedQueryMultipleDerivedJoinsDescribeAndExecute` | Direct | Confirms Describe Statement/Portal metadata matches the current star-expansion behavior, including `_lsn`, derived-table window aliases, and aliased projections across chained derived joins. |
| `INSERT ... RETURNING ...` | `internal/engine/executor/engine_test.go`: early `INSERT ... RETURNING id` coverage near the file start; `TestRowHistoryWithUUIDAndReturning`; `internal/engine/parser/parser_test.go`: `TestParseInsertReturning`, `TestParseInsertReturningMultiple`, `TestParseInsertReturningStar`; `internal/server/pgwire/extended_query_conformance_test.go`: `TestExtendedQueryInsertReturningUsesSchemaAwareRowDescription` | Direct | Parser and executor cover insert-focused `RETURNING`, and pgwire conformance coverage now asserts schema-aware RowDescription OIDs for extended-query `INSERT ... RETURNING`. |
| `INSERT ... ON CONFLICT ...` | `internal/engine/executor/engine_test.go`: `TestInsertOnConflictDoNothing`, `TestInsertOnConflictDoUpdate`, `TestInsertOnConflictExcludedColumn` | Direct | Covers current `DO NOTHING`, `DO UPDATE`, and `EXCLUDED` shapes. |
| `TRUNCATE TABLE` | `internal/engine/executor/engine_test.go`: `TestTruncateTableBasic` | Direct | Confirms truncation clears rows while keeping the table usable. |
| `DROP TABLE IF EXISTS` / `DROP INDEX IF EXISTS` | `internal/engine/executor/engine_test.go`: `TestDropTableIfExistsNonExistent`, `TestDropIndexIfExistsNonExistent` | Direct | Confirms deterministic no-error behavior for absent objects. |

## Guardrail/error-path evidence

| Public claim | Evidence | Status | Notes |
|---|---|---|---|
| Bare `BEGIN` is rejected with ASQL transaction guidance | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilityUnsupportedPatternGuidance` | Direct | Confirms explicit steer toward `BEGIN DOMAIN ...` / `BEGIN CROSS DOMAIN ...`. |
| `ANY(...)` / `ARRAY[...]` assumptions are rejected with actionable guidance | `internal/server/pgwire/server_test.go`: `TestPGWireCompatibilityUnsupportedPatternGuidance` | Direct | Matches the compatibility-matrix guardrail language. |

For the fuller current pgwire error/SQLSTATE picture — including
end-to-end-covered cancellation, transaction-state, object/constraint, and
input cases plus the remaining unit-covered mapper behavior — see
[pgwire-error-sqlstate-behavior-v1.md](pgwire-error-sqlstate-behavior-v1.md).

## Evidence gaps to close next

There are no open claim-to-test evidence gaps in the current v1 compatibility
pack.

Future public compatibility claims should only be added when docs, regression
tests, and evidence-map entries land together.