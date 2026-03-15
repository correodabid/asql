# ASQL Release Evidence Bundle Template — v1.0.0-rc1

Status: template
Use for: the first release-candidate review bundle aligned with the GA compatibility contract.

Use together with:
- [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)
- [ga-compatibility-freeze-procedure-v1.md](ga-compatibility-freeze-procedure-v1.md)
- [../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)
- [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md)

---

Release/tag: `v1.0.0-rc1`
Date: `<yyyy-mm-dd>`
Owner: `<name>`
Commit: `<sha>`
Decision: `proceed | blocked`

## 1. GA contract review

Contract reviewed:
- `asql-ga-compatibility-contract-v1.md`: `yes | no`

Buckets touched:
- SQL / pgwire: `unchanged | additive | risk`
- WAL / replay: `unchanged | additive | risk`
- backup / restore / upgrade: `unchanged | additive | risk`
- operator surface: `unchanged | additive | risk`
- cluster continuity: `unchanged | additive | risk | n/a`
- performance guardrails: `green | blocked`

Docs reviewed:
- `README.md`: `yes | no`
- `docs/getting-started/`: `yes | no`
- compatibility policy/surface/evidence docs: `yes | no`
- operations docs: `yes | no`
- `site/`: `yes | no`

Contract notes:
- `<summary>`

## 2. Runtime

Status: `green | green-with-notes | blocked`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestRuntimeAndAdminHTTPSmokeFlow`: `pass | fail | n/a`
- `internal/server/pgwire/server_test.go`
  - `TestPGWireSimpleQueryRoundtrip`: `pass | fail | n/a`
  - `TestPGWireCompatibilitySupportedPatterns`: `pass | fail | n/a`

Commands / lanes:
- `<exact commands or CI job names>`

Notes:
- `<summary>`

## 3. Compatibility

Status: `green | green-with-notes | blocked`

GA contract review result:
- compatible with frozen contract: `yes | no`

Evidence lanes:
- Lane A (`pgx` simple protocol baseline): `pass | fail | n/a`
- Lane B (extended query baseline): `pass | fail | n/a`
- Lane C (`psql` baseline): `pass | fail | n/a`
- Lane D (JDBC / GUI baseline): `pass | fail | n/a`
- Lane E (`COPY` baseline): `pass | fail | n/a`
- Lane F (SQLSTATE / error-shape baseline): `pass | fail | n/a`

Docs reviewed:
- `sql-pgwire-compatibility-policy-v1.md`: `yes | no`
- `postgres-compatibility-surface-v1.md`: `yes | no`
- `postgres-compatibility-evidence-v1.md`: `yes | no`
- `pgwire-error-sqlstate-behavior-v1.md`: `yes | no`

Notes:
- `<summary>`

## 4. WAL / replay / recovery

Status: `green | green-with-notes | blocked`

Evidence:
- `test/integration/restart_replay_test.go`
  - `TestRestartReplayRestoresState`: `pass | fail | n/a`
- `test/integration/backup_restore_test.go`
  - `TestBackupWipeRestorePreservesQueryParity`: `pass | fail | n/a`
- `test/integration/recovery_restore_test.go`
  - `TestBaseBackupRestoreToLSNAndTimestamp`: `pass | fail | n/a`
  - `TestBaseBackupVerificationFailsOnChecksumMismatch`: `pass | fail | n/a`
- WAL compatibility/version tests: `pass | fail | n/a`
- upgrade simulation: `pass | fail | n/a`

Notes:
- `<summary>`

## 5. Operations

Status: `green | green-with-notes | blocked`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestAdminMetricsExposeFailoverLeaderAndSafeLSN`: `pass | fail | n/a`
  - `TestAdminReadyzAndLeadershipEndpoints`: `pass | fail | n/a`
  - `TestAdminRecoveryInspectionAndValidationEndpoints`: `pass | fail | n/a`

Docs reviewed:
- `runbook.md`: `yes | no`
- `incident-runbook-v1.md`: `yes | no`
- `telemetry-dashboard-v1.md`: `yes | no`
- `slo-v1.md`: `yes | no`

Notes:
- `<summary>`

## 6. Replicated-runtime continuity

Status: `green | green-with-notes | blocked | n/a`

Use when cluster/runtime, fencing, promotion, or replicated commit behavior changed.

Evidence:
- `test/integration/failover_simulation_test.go`
  - `TestFailoverSimulationLeaderCrashPromotesCandidate`: `pass | fail | n/a`
  - `TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken`: `pass | fail | n/a`
  - `TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence`: `pass | fail | n/a`
- `test/integration/failover_state_hash_continuity_test.go`
  - `TestFailoverPromotionPreservesReplayStateHashContinuity`: `pass | fail | n/a`

Notes:
- `<summary>`

## 7. Performance guardrails

Status: `green | green-with-notes | blocked`

Evidence:
- single-node write scaling guardrail: `pass | fail`
- cluster append-growth guardrail: `pass | fail`
- benchmark baseline review completed: `yes | no`

Commands:
- `<exact commands>`

Notes:
- `<summary>`

## 8. Release artifacts

Status: `green | green-with-notes | blocked`

Artifacts:
- binaries built: `yes | no`
- checksums generated: `yes | no`
- SBOM generated: `yes | no`
- signatures generated: `yes | no`
- release bundle uploaded: `yes | no`

Notes:
- `<summary>`

## 9. Blockers and follow-up

P0/P1 blockers:
- `<none or list>`

Narrowed claims for this release:
- `<none or list>`

Required follow-up before GA:
- `<none or list>`

## 10. Final recommendation

Recommendation: `proceed | blocked`

Reason:
- `<short decision summary>`
