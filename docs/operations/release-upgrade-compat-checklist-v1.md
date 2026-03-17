# ASQL Release Candidate, Upgrade & Backward-Compatibility Checklist (v1)

Date: 2026-02-28
Applies to: every release candidate and GA release.

## Goal

Ensure release candidates and GA upgrades are safe, observable, and stable enough to ship.

Use [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md) as the
recommended format for the short release evidence summary referenced below.

For the contract being validated, use
[../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)
together with the pgwire compatibility policy and surface docs, plus the
operational review in
[ga-compatibility-freeze-procedure-v1.md](ga-compatibility-freeze-procedure-v1.md).

## Release-candidate gate (`v1.0.0-rc*` and later)

Every release candidate should include a short evidence bundle covering:

1. Runtime sanity:
   - `cmd/asqld` boots locally.
   - the pgwire getting-started flow still works.
   - Studio launch path still works when included in release scope.
   - concrete smoke evidence should include `internal/server/pgwire/admin_http_test.go`: `TestRuntimeAndAdminHTTPSmokeFlow` whenever runtime/admin boot wiring changes.
2. Operator sanity:
   - `/livez`, `/readyz`, and `/metrics` respond on the admin HTTP surface.
   - leadership, failover history, and WAL retention admin endpoints respond with expected shapes.
   - concrete endpoint evidence should include `internal/server/pgwire/admin_http_test.go`: `TestAdminReadyzAndLeadershipEndpoints`, `TestAdminMetricsExposeFailoverLeaderAndSafeLSN`, and `TestRuntimeAndAdminHTTPSmokeFlow` when admin HTTP behavior changes.
3. Compatibility sanity:
   - the release is reviewed against the GA contract in [../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md).
   - compatibility docs were reviewed in the same release window.
   - documented mainstream client/tool flows still pass the current pack in [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md).
   - when pgwire compatibility claims or app-facing adoption docs changed, the Epic AI prioritized mainstream smoke matrix in [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md) is included in the release evidence summary.
   - if pgwire error handling or startup/auth code changed, the SQLSTATE/error-shape lane is green in the same release window.
4. Recovery sanity:
   - replay/restart parity suite passes.
   - backup/restore parity suite passes.
   - failover continuity suite passes for the supported replicated runtime path.
   - concrete recovery evidence should include `test/integration/restart_replay_test.go`: `TestRestartReplayRestoresState`, `test/integration/backup_restore_test.go`: `TestBackupWipeRestorePreservesQueryParity`, and `test/integration/recovery_restore_test.go`: `TestBaseBackupRestoreToLSNAndTimestamp`.
   - concrete replicated-runtime continuity evidence should include `test/integration/failover_simulation_test.go`: `TestFailoverSimulationLeaderCrashPromotesCandidate`, `TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken`, `TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence`, and `test/integration/failover_state_hash_continuity_test.go`: `TestFailoverPromotionPreservesReplayStateHashContinuity`.
5. Documentation sanity:
   - `README.md`, `docs/getting-started/`, `docs/reference/`, and `site/` were updated when user-visible behavior changed.

## Pre-release validation

1. CI baseline green:
   - `go test ./...`
   - race lane
   - security scan lane
2. Determinism checks green:
   - replay equivalence suite
   - at minimum, `go test ./test/integration -run 'TestRestartReplayRestoresState|TestBackupWipeRestorePreservesQueryParity|TestBaseBackupRestoreToLSNAndTimestamp' -v`
3. WAL compatibility tests green:
   - legacy fixture read
   - version mismatch rejection
   - append sequence continuity
4. Performance guardrails green:
   - single-node write scaling guardrail
   - cluster append-growth guardrail
5. Production-facing smoke lanes green:
   - pgwire onboarding flow
   - admin HTTP health/metrics flow
   - pgwire compatibility pack baseline lanes
   - prioritized mainstream compatibility smoke matrix (`pgx`, `psql`, DBeaver/DataGrip, `pgAdmin`, ORM-lite, BI-lite) when the release touched compatibility-facing code or docs
   - focused pgwire SQLSTATE/error-shape regressions when protocol error behavior changed
   - focused failover continuity evidence when cluster/runtime, fencing, or promotion logic changed
   - release-candidate evidence summary generated

## Upgrade simulation

Run on representative datasets:

1. Start previous version and generate WAL history.
2. Stop previous version cleanly.
3. Start candidate version with same WAL.
4. Validate:
   - engine startup succeeds,
   - replay succeeds,
   - time-travel queries return expected row parity.
5. Current executable proxy lane for this workflow:
   - `test/integration/upgrade_simulation_test.go`: `TestUpgradeSimulationCandidateReplaysPreviousWALAndPreservesHistoricalParity`

## Backup/restore gate

1. Create backup from candidate-compatible WAL.
2. Wipe primary WAL.
3. Restore from backup.
4. Execute parity test:
   - `backup -> wipe -> restore -> query parity`.
5. Prefer the repository's current executable evidence set as the default gate:
   - `test/integration/backup_restore_test.go`: `TestBackupWipeRestorePreservesQueryParity`
   - `test/integration/recovery_restore_test.go`: `TestBaseBackupRestoreToLSNAndTimestamp`
   - `test/integration/recovery_restore_test.go`: `TestBaseBackupVerificationFailsOnChecksumMismatch`

## API contract checks

1. Verify the canonical pgwire / SQL path remains functional:
   - `BEGIN DOMAIN`, `BEGIN CROSS DOMAIN`, `COMMIT`, and `ROLLBACK`.
   - core DDL/DML flows used by getting-started and fixture import paths.
   - historical queries through `AS OF LSN`, `FOR HISTORY`, and temporal helper functions.
2. If the optional gRPC/admin surface is part of the shipped release, verify command paths remain functional:
   - `BeginTx`, `Execute`, `CommitTx`, `RollbackTx`, `TimeTravelQuery`, `ReplayToLSN`.
3. Validate auth modes across enabled listeners:
   - token mode,
   - TLS / mTLS for pgwire,
   - TLS / mTLS for gRPC where enabled.

## Operational checks

1. Audit log fields present and structured.
2. `/metrics` exports the expected Prometheus baseline for health, WAL durability, replay, snapshot, lag, and failover.
3. SLO/dashboard consumers still map cleanly to emitted metrics.
4. Incident runbook references updated for new behavior.

## Release decision gate

Release can proceed only if all below are true:

- No unresolved P0/P1 compatibility issues.
- Upgrade simulation passes across test matrix.
- Backup/restore parity test passes.
- Replicated-runtime failover continuity tests pass for the supported cluster path.
- Rollback plan documented and verified.

## Required release artifacts

- Test evidence summary
- Release evidence bundle summary (recommended format: [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md))
- GA compatibility contract review result
- Release-candidate gate summary (`runtime`, `compatibility`, `recovery`, `operations`)
- Compatibility matrix result
- Upgrade guide notes
- Rollback plan notes