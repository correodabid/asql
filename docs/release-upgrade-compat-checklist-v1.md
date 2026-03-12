# ASQL Release Upgrade & Backward-Compatibility Checklist (v1)

Date: 2026-02-28
Applies to: every release candidate and GA release.

## Goal

Ensure upgrades are safe and WAL/protocol compatibility remains stable.

## Pre-release validation

1. CI baseline green:
   - `go test ./...`
   - race lane
   - security scan lane
2. Determinism checks green:
   - replay equivalence suite
3. WAL compatibility tests green:
   - legacy fixture read
   - version mismatch rejection
   - append sequence continuity

## Upgrade simulation

Run on representative datasets:

1. Start previous version and generate WAL history.
2. Stop previous version cleanly.
3. Start candidate version with same WAL.
4. Validate:
   - engine startup succeeds,
   - replay succeeds,
   - time-travel queries return expected row parity.

## Backup/restore gate

1. Create backup from candidate-compatible WAL.
2. Wipe primary WAL.
3. Restore from backup.
4. Execute parity test:
   - `backup -> wipe -> restore -> query parity`.

## API contract checks

1. Verify gRPC command paths remain functional:
   - BeginTx, Execute, CommitTx, RollbackTx, TimeTravelQuery, ReplayToLSN.
2. Validate auth modes:
   - token mode,
   - mTLS mode.

## Operational checks

1. Audit log fields present and structured.
2. SLO dashboard baseline metrics available.
3. Incident runbook references updated for new behavior.

## Release decision gate

Release can proceed only if all below are true:

- No unresolved P0/P1 compatibility issues.
- Upgrade simulation passes across test matrix.
- Backup/restore parity test passes.
- Rollback plan documented and verified.

## Required release artifacts

- Test evidence summary
- Compatibility matrix result
- Upgrade guide notes
- Rollback plan notes