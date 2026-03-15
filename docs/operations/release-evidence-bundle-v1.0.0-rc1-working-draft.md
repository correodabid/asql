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

Status: `blocked`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestRuntimeAndAdminHTTPSmokeFlow`: `n/a`
- `internal/server/pgwire/server_test.go`
  - `TestPGWireSimpleQueryRoundtrip`: `n/a`
  - `TestPGWireCompatibilitySupportedPatterns`: `n/a`

Commands / lanes:
- pending first real RC evidence pass

Notes:
- No runtime smoke lane was executed as part of this documentation/launch-alignment window.
- This section must be filled with real evidence before `rc1` can proceed.

## 3. Compatibility

Status: `blocked`

GA contract review result:
- compatible with frozen contract: `yes, on documentation scope only`

Evidence lanes:
- Lane A (`pgx` simple protocol baseline): `n/a`
- Lane B (extended query baseline): `n/a`
- Lane C (`psql` baseline): `n/a`
- Lane D (JDBC / GUI baseline): `n/a`
- Lane E (`COPY` baseline): `n/a`
- Lane F (SQLSTATE / error-shape baseline): `n/a`

Docs reviewed:
- `sql-pgwire-compatibility-policy-v1.md`: `yes`
- `postgres-compatibility-surface-v1.md`: `yes`
- `postgres-compatibility-evidence-v1.md`: `yes`
- `pgwire-error-sqlstate-behavior-v1.md`: `yes`

Notes:
- The compatibility claim hierarchy is now better defined and linked publicly.
- The compatibility lanes themselves have not yet been rerun in this review window, so this release cannot claim a green compatibility gate yet.

## 4. WAL / replay / recovery

Status: `blocked`

Evidence:
- `test/integration/restart_replay_test.go`
  - `TestRestartReplayRestoresState`: `n/a`
- `test/integration/backup_restore_test.go`
  - `TestBackupWipeRestorePreservesQueryParity`: `n/a`
- `test/integration/recovery_restore_test.go`
  - `TestBaseBackupRestoreToLSNAndTimestamp`: `n/a`
  - `TestBaseBackupVerificationFailsOnChecksumMismatch`: `n/a`
- WAL compatibility/version tests: `n/a`
- upgrade simulation: `n/a`

Notes:
- Recovery/replay claims remain in place at the documentation level.
- The executable recovery evidence still needs to be gathered for this actual RC review.

## 5. Operations

Status: `blocked`

Evidence:
- `internal/server/pgwire/admin_http_test.go`
  - `TestAdminMetricsExposeFailoverLeaderAndSafeLSN`: `n/a`
  - `TestAdminReadyzAndLeadershipEndpoints`: `n/a`
  - `TestAdminRecoveryInspectionAndValidationEndpoints`: `n/a`

Docs reviewed:
- `runbook.md`: `yes`
- `incident-runbook-v1.md`: `yes`
- `telemetry-dashboard-v1.md`: `yes`
- `slo-v1.md`: `yes`

Notes:
- Operator-facing docs and site visibility improved in this window.
- Admin/metrics evidence remains pending for the real RC pass.

## 6. Replicated-runtime continuity

Status: `blocked`

Use when cluster/runtime, fencing, promotion, or replicated commit behavior changed.

Evidence:
- `test/integration/failover_simulation_test.go`
  - `TestFailoverSimulationLeaderCrashPromotesCandidate`: `n/a`
  - `TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken`: `n/a`
  - `TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence`: `n/a`
- `test/integration/failover_state_hash_continuity_test.go`
  - `TestFailoverPromotionPreservesReplayStateHashContinuity`: `n/a`

Notes:
- No new failover-runtime code was changed in this documentation-focused window.
- Even so, the RC gate still requires continuity evidence before proceeding.

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
- runtime smoke evidence not yet captured for this RC review window
- compatibility pack lanes not yet executed for this RC review window
- replay/recovery evidence not yet captured for this RC review window
- admin/operations evidence not yet captured for this RC review window
- release artifact generation not yet executed

Narrowed claims for this release:
- ASQL remains explicitly positioned as a deterministic SQL engine with a pragmatic PostgreSQL-compatible subset over pgwire, not a drop-in PostgreSQL replacement.

Required follow-up before GA:
- run and record the runtime smoke lanes
- run and record pgwire compatibility pack lanes
- run and record replay/recovery/backup evidence
- run and record admin and failover continuity evidence
- generate actual release artifacts and final release notes bundle

## 10. Final recommendation

Recommendation: `blocked`

Reason:
- The product/docs/launch story is much stronger and the guardrails are in better shape, but this is still only a working RC bundle draft.
- The first full evidence pass has not yet been executed, so the release should remain blocked until the required runtime, compatibility, recovery, operations, and artifact lanes are captured.
