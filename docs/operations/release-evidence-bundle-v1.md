# ASQL Release Evidence Bundle v1

## Purpose

This note turns the release checklist into a small, reusable evidence bundle
format for release candidates and GA releases.

Use it together with:

- [../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)
- [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md)
- [runbook.md](runbook.md)
- [../reference/postgres-compatibility-evidence-v1.md](../reference/postgres-compatibility-evidence-v1.md)

The goal is not to paste every CI log. The goal is to capture a compact,
reviewable summary that shows which release-critical lanes were executed,
what evidence backed them, and whether docs stayed aligned.

## When to produce it

Create one bundle for:

- every release candidate,
- every GA release,
- any release cut that changes pgwire protocol behavior, recovery behavior,
  or the supported clustered runtime path.

## Minimum bundle shape

Each bundle should contain five sections:

1. `runtime`
2. `compatibility`
3. `recovery`
4. `operations`
5. `artifacts`

For each section, record:

- status: `green`, `green-with-notes`, or `blocked`
- date
- commit / tag under evaluation
- exact tests, lanes, or commands used
- brief notes about any narrowed claims or follow-up items

## Recommended evidence set

### 1. Runtime

Minimum evidence:

- `internal/server/pgwire/admin_http_test.go`
  - `TestRuntimeAndAdminHTTPSmokeFlow`
- `internal/server/pgwire/server_test.go`
  - `TestPGWireSimpleQueryRoundtrip`
  - `TestPGWireCompatibilitySupportedPatterns`

What this proves:

- the canonical pgwire runtime boots,
- the main getting-started SQL path still works,
- admin HTTP is reachable and sane,
- the basic app-facing query path still behaves as documented.

### 2. Compatibility

Minimum evidence:

- baseline lanes from [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md)
- if error handling changed:
  - `internal/server/pgwire/sqlstate_regression_test.go`
  - `internal/server/pgwire/errors_test.go`

What this proves:

- public PostgreSQL-compatibility claims are still backed by passing evidence,
- startup/session/catalog flows still match the documented subset,
- SQLSTATE claims remain aligned with actual wire behavior when touched.

### 3. Recovery

Minimum evidence:

- `test/integration/restart_replay_test.go`
  - `TestRestartReplayRestoresState`
- `test/integration/backup_restore_test.go`
  - `TestBackupWipeRestorePreservesQueryParity`
- `test/integration/recovery_restore_test.go`
  - `TestBaseBackupRestoreToLSNAndTimestamp`
  - `TestBaseBackupVerificationFailsOnChecksumMismatch`

What this proves:

- restart/replay still restores state,
- backup/wipe/restore preserves query parity,
- restore boundaries still work,
- corruption is still detected before unsafe restore proceeds.

### 4. Operations

Minimum evidence:

- `internal/server/pgwire/admin_http_test.go`
  - `TestAdminMetricsExposeFailoverLeaderAndSafeLSN`
  - `TestAdminReadyzAndLeadershipEndpoints`
  - `TestAdminRecoveryInspectionAndValidationEndpoints`
- docs reviewed when the operator surface changed:
  - [runbook.md](runbook.md)
  - [incident-runbook-v1.md](incident-runbook-v1.md)
  - [telemetry-dashboard-v1.md](telemetry-dashboard-v1.md)

What this proves:

- the operator-facing health/metrics/admin surface still responds with the
  expected shapes,
- operator docs remain aligned with the actual runtime surface.

### 5. Replicated-runtime continuity

Include this whenever cluster/runtime, fencing, or promotion behavior changed.

Minimum evidence:

- `test/integration/failover_simulation_test.go`
  - `TestFailoverSimulationLeaderCrashPromotesCandidate`
  - `TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken`
  - `TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence`
- `test/integration/failover_state_hash_continuity_test.go`
  - `TestFailoverPromotionPreservesReplayStateHashContinuity`

What this proves:

- failover still promotes a valid candidate,
- stale leaders remain fenced,
- seeded failover behavior stays deterministic,
- promotion does not diverge replayed state.

### 6. Release artifacts

Record whether the release bundle artifacts were produced successfully.

Current CI release-bundle evidence is generated in:

- [.github/workflows/ci.yml](../../.github/workflows/ci.yml)

Capture at least:

- binaries built,
- checksums generated,
- SBOM generated,
- signatures generated,
- release bundle artifact uploaded.

## Suggested compact template

```text
Release/tag: <tag-or-commit>
Date: <yyyy-mm-dd>
Owner: <name>

runtime: green
- evidence:
  - TestRuntimeAndAdminHTTPSmokeFlow
  - TestPGWireSimpleQueryRoundtrip
  - TestPGWireCompatibilitySupportedPatterns
- notes: <optional>

compatibility: green
- GA contract review:
  - asql-ga-compatibility-contract-v1.md reviewed: yes/no
- evidence:
  - pgwire compatibility lanes A/B/C-or-D
  - SQLSTATE lane (if changed)
- docs reviewed:
  - postgres-compatibility-surface-v1.md
  - postgres-compatibility-evidence-v1.md
- notes: <optional>

recovery: green
- evidence:
  - TestRestartReplayRestoresState
  - TestBackupWipeRestorePreservesQueryParity
  - TestBaseBackupRestoreToLSNAndTimestamp
  - TestBaseBackupVerificationFailsOnChecksumMismatch
- notes: <optional>

operations: green
- evidence:
  - TestAdminMetricsExposeFailoverLeaderAndSafeLSN
  - TestAdminReadyzAndLeadershipEndpoints
  - TestAdminRecoveryInspectionAndValidationEndpoints
- docs reviewed:
  - runbook.md
  - incident-runbook-v1.md
- notes: <optional>

replicated-runtime continuity: green|n/a
- evidence:
  - TestFailoverSimulationLeaderCrashPromotesCandidate
  - TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken
  - TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence
  - TestFailoverPromotionPreservesReplayStateHashContinuity
- notes: <optional>

artifacts: green
- binaries: yes/no
- checksums: yes/no
- sbom: yes/no
- signatures: yes/no
- uploaded bundle: yes/no

release decision: proceed|blocked
blockers:
- <none or list>
```

## Rule of use

If a lane is red:

- do not silently widen claims,
- either fix the regression,
- or narrow docs in the same change window,
- and record the decision in the bundle notes.
