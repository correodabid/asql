# ASQL Release Evidence Bundle — v1.0.0-rc1 Working Draft

Status: working draft
Use for: the first real release-candidate evidence pass before the final `v1.0.0-rc1` decision.

Use together with:
- [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md)
- [release-evidence-bundle-v1.0.0-rc1-template.md](release-evidence-bundle-v1.0.0-rc1-template.md)
- [ga-compatibility-freeze-procedure-v1.md](ga-compatibility-freeze-procedure-v1.md)
- [../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)

---

Release/tag: `v1.0.0-rc1`
Date: `2026-03-15`
Owner: `working draft`
Commit: `3418a8c`
Decision: `blocked`

## 1. GA contract review

Contract reviewed:
- `asql-ga-compatibility-contract-v1.md`: `yes`

Buckets touched:
- SQL / pgwire: `additive`
- WAL / replay: `unchanged`
- backup / restore / upgrade: `unchanged`
- operator surface: `additive`
- cluster continuity: `unchanged`
- performance guardrails: `green`

Docs reviewed:
- `README.md`: `yes`
- `docs/getting-started/`: `yes`
- compatibility policy/surface/evidence docs: `yes`
- operations docs: `yes`
- `site/`: `yes`

Contract notes:
- This change window primarily hardened the public contract and release process: GA compatibility contract, freeze procedure, RC template, launch narrative, examples packaging, and public-site alignment.
- The contract and front-door surfaces are now materially clearer.
- The release remains blocked because a full RC evidence run has not yet been executed in this review window.

## 2. Runtime

Status: `green`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestRuntimeAndAdminHTTPSmokeFlow`: `pass`
- `internal/server/pgwire/server_test.go`
  - `TestPGWireSimpleQueryRoundtrip`: `pass`
  - `TestPGWireCompatibilitySupportedPatterns`: `pass`

Commands / lanes:
- `go test ./internal/server/pgwire -run 'TestRuntimeAndAdminHTTPSmokeFlow|TestPGWireSimpleQueryRoundtrip|TestPGWireCompatibilitySupportedPatterns|TestPGWireForHistoryRegressionStableMetadataAndRows|TestExtendedQueryPortalResumesAcrossExecuteCalls|TestExtendedQueryDescribeStatementInfersParameterCount|TestExtendedQueryDescribeStatementInfersInsertParameterOIDs|TestExtendedQueryDescribeStatementInfersPredicateParameterOIDs|TestExtendedQueryDescribeStatementInfersUpdateParameterOIDs|TestExtendedQueryDescribeStatementInfersArithmeticUpdateParameterOIDs|TestExtendedQueryInsertReturningUsesSchemaAwareRowDescription|TestExtendedQueryDiscardsMessagesUntilSyncAfterError|TestExtendedQueryBinaryBindSupportsInt4Int8AndBool|TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable|TestSSLModePreferFallback|TestCatalogStartupIntrospectionQueries|TestCatalogEmptyInterceptsExposeSchemaAcrossProtocols|TestPGWireCompatibilityUnsupportedPatternGuidance|TestShowUnknownParamFallbackWorksOnExtendedProtocol|TestCopyFromStdinInsertsRowsAndAcceptsChunkedCopyData|TestCopyToStdoutStreamsRows|TestCopyFromStdinCSVInsertsQuotedValues|TestCopyToStdoutCSVQuotesValues|TestCopyFailRollsBackInsertedRows|TestPGWirePasswordAuthenticationWrongPasswordReturns28P01|TestPGWirePasswordAuthenticationWrongMessageReturns08P01|TestSendFollowerRedirectErrorWrites25006AndHint|TestPGWireTransactionStateSQLStates|TestPGWireObjectAndConstraintSQLStates|TestSQLStateFromMessageMappings|TestMapErrorToSQLState' -count=1`

Notes:
- Runtime smoke evidence was captured in the current review window.
- The canonical pgwire runtime smoke lane is now backed by a targeted passing test run in `internal/server/pgwire`.

## 3. Compatibility

Status: `green`

GA contract review result:
- compatible with frozen contract: `yes`

Evidence lanes:
- Lane A (`pgx` simple protocol baseline): `pass`
- Lane B (extended query baseline): `pass`
- Lane C (`psql` baseline): `pass`
- Lane D (JDBC / GUI baseline): `pass`
- Lane E (`COPY` baseline): `pass`
- Lane F (SQLSTATE / error-shape baseline): `pass`

Docs reviewed:
- `sql-pgwire-compatibility-policy-v1.md`: `yes`
- `postgres-compatibility-surface-v1.md`: `yes`
- `postgres-compatibility-evidence-v1.md`: `yes`
- `pgwire-error-sqlstate-behavior-v1.md`: `yes`

Notes:
- The compatibility claim hierarchy is now better defined and linked publicly.
- The pgwire compatibility evidence pack lanes covered by the targeted `internal/server/pgwire` run are green in this review window.
- This clears the runtime + compatibility portion of the working RC bundle, while recovery, operations, replicated-runtime continuity, and release artifacts remain pending.

## 4. WAL / replay / recovery

Status: `green`

Evidence:
- `test/integration/restart_replay_test.go`
  - `TestRestartReplayRestoresState`: `pass`
- `test/integration/backup_restore_test.go`
  - `TestBackupWipeRestorePreservesQueryParity`: `pass`
- `test/integration/recovery_restore_test.go`
  - `TestBaseBackupRestoreToLSNAndTimestamp`: `pass`
  - `TestBaseBackupVerificationFailsOnChecksumMismatch`: `pass`
- WAL compatibility/version tests: `pass`
- upgrade simulation: `pass`

Notes:
- Recovery and restore evidence was captured in the current review window.
- Command used: `go test ./test/integration -run 'TestRestartReplayRestoresState|TestBackupWipeRestorePreservesQueryParity|TestBaseBackupRestoreToLSNAndTimestamp|TestBaseBackupVerificationFailsOnChecksumMismatch' -count=1`
- WAL compatibility/version evidence was captured in the current review window.
- Command used: `go test ./internal/storage/wal -run 'TestFileLogStoreAppendAndReadFrom|TestFileLogStoreRecoverAfterReopen|TestFileLogStoreRejectsUnsupportedFutureVersion' -count=1`
- Covered by that lane:
  - append sequence continuity via `TestFileLogStoreAppendAndReadFrom`
  - reopen/recovery continuity via `TestFileLogStoreRecoverAfterReopen`
  - version mismatch rejection via `TestFileLogStoreRejectsUnsupportedFutureVersion`
- I did not find a dedicated automated legacy WAL fixture-read lane in the current repository.
- An explicit executable upgrade-simulation proxy lane now exists and passed in the current review window.
- Command used: `go test ./test/integration -run TestUpgradeSimulationCandidateReplaysPreviousWALAndPreservesHistoricalParity -v`
- Test: `test/integration/upgrade_simulation_test.go`: `TestUpgradeSimulationCandidateReplaysPreviousWALAndPreservesHistoricalParity`

## 5. Operations

Status: `green`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestAdminMetricsExposeFailoverLeaderAndSafeLSN`: `pass`
  - `TestAdminReadyzAndLeadershipEndpoints`: `pass`
  - `TestAdminRecoveryInspectionAndValidationEndpoints`: `pass`

Docs reviewed:
- `runbook.md`: `yes`
- `incident-runbook-v1.md`: `yes`
- `telemetry-dashboard-v1.md`: `yes`
- `slo-v1.md`: `yes`

Notes:
- Operator-facing docs and site visibility improved in this window.
- Admin/metrics evidence was captured in the current review window.
- Command used: `go test ./internal/server/pgwire -run 'TestAdminMetricsExposeFailoverLeaderAndSafeLSN|TestAdminReadyzAndLeadershipEndpoints|TestAdminRecoveryInspectionAndValidationEndpoints' -count=1`

## 6. Replicated-runtime continuity

Status: `green`

Use when cluster/runtime, fencing, promotion, or replicated commit behavior changed.

Evidence:
- `test/integration/failover_simulation_test.go`
  - `TestFailoverSimulationLeaderCrashPromotesCandidate`: `pass`
  - `TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken`: `pass`
  - `TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence`: `pass`
- `test/integration/failover_state_hash_continuity_test.go`
  - `TestFailoverPromotionPreservesReplayStateHashContinuity`: `pass`

Notes:
- Replicated-runtime continuity evidence was captured in the current review window.
- Command used: `go test ./test/integration -run 'TestFailoverSimulationLeaderCrashPromotesCandidate|TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken|TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence|TestFailoverPromotionPreservesReplayStateHashContinuity' -count=1`
- This clears the failover/continuity lane for the working RC draft.

## 7. Performance guardrails

Status: `green-with-notes`

Evidence:
- single-node write scaling guardrail: `pass`
- cluster append-growth guardrail: `pass`
- benchmark baseline review completed: `yes`

Commands:
- `make bench-write-scaling-guardrail`
- latest validated result in this work window: tail ratio `1.54` within threshold `1.75`
- `make bench-append-growth-cluster-guardrail`
- latest validated result in this work window: cluster append-growth guardrail passed at approximately `x1.05`

Notes:
- This was the strongest fully exercised evidence area in the current work window.
- The write-scaling and append-growth guardrails were stabilized and documented.
- Performance evidence is green locally, but the rest of the RC evidence set is still incomplete.

## 8. Release artifacts

Status: `blocked`

Artifacts:
- binaries built: `no`
- checksums generated: `no`
- SBOM generated: `no`
- signatures generated: `no`
- release bundle uploaded: `no`

Notes:
- This working draft is documentation-first and not a final release artifact run.

## 9. Blockers and follow-up

P0/P1 blockers:
- release artifact generation not yet executed

Narrowed claims for this release:
- ASQL remains explicitly positioned as a deterministic SQL engine with a pragmatic PostgreSQL-compatible subset over pgwire, not a drop-in PostgreSQL replacement.

Required follow-up before GA:
- generate actual release artifacts and final release notes bundle

## 10. Final recommendation

Recommendation: `blocked`

Reason:
- The product/docs/launch story is much stronger and the RC draft now includes real runtime, compatibility, recovery, operations, continuity, and performance evidence.
- The release still remains blocked until the release artifact lanes are captured.
