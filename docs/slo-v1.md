# ASQL SLO Definitions (v1)

Date: 2026-02-28
Scope: single-node and optional replication baseline.

## SLO-1: Determinism integrity

Objective:
- Replay equivalence success rate >= 99.9% (rolling 7 days).

SLI:
- `replay_equivalence_passes / replay_equivalence_runs`

Alert:
- Warning: < 99.95% for 24h
- Critical: < 99.9% for 24h

## SLO-2: Crash recovery success

Objective:
- Successful restart recovery >= 99.0% (rolling 7 days).

SLI:
- `recovery_success_count / recovery_attempt_count`

Alert:
- Warning: < 99.5% for 24h
- Critical: < 99.0% for 24h

## SLO-3: Replication catch-up reliability

Objective:
- Catch-up success >= 99.0% (rolling 7 days) in enabled replication mode.

SLI:
- `replication_catchup_success / replication_catchup_attempts`

Alert:
- Warning: < 99.5% for 24h
- Critical: < 99.0% for 24h

## SLO-4: Onboarding success

Objective:
- Smoke onboarding pass rate >= 95% (rolling 7 days).

SLI:
- `smoke_onboarding_passes / smoke_onboarding_runs`

Alert:
- Warning: < 97% for 24h
- Critical: < 95% for 24h

## SLO-5: Time-to-first-transaction

Objective:
- <= 15 minutes from clean machine to first successful transaction.

SLI:
- p95 of measured onboarding duration from smoke workflow.

Alert:
- Warning: p95 > 12 min (rolling 7 days)
- Critical: p95 > 15 min (rolling 7 days)

## Measurement notes

- Use nightly CI plus pilot environment runs as data sources.
- Treat missing telemetry as failed measurement windows.
- Report weekly summary in ops review.