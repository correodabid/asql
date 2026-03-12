# ASQL Telemetry Dashboard Baseline (v1)

Date: 2026-02-28

This dashboard baseline defines the minimum operational views for Sprint 3.

## Panel group A: Reliability

1. Replay equivalence pass rate (7d)
2. Crash recovery success rate (7d)
3. Replay-to-LSN operation failures (rate)

## Panel group B: Replication health

1. Catch-up success rate (7d)
2. Replication lag by follower (LSN delta)
3. Divergence detection events (count/rate)

## Panel group C: Performance baseline

1. p95 write latency (reference workload)
2. p95 read latency (reference workload)
3. Replay duration histogram

## Panel group D: Onboarding and DX

1. Smoke onboarding pass rate
2. Time-to-first-successful-transaction p95
3. CI deterministic suite status trend

## Alert baseline

- Replay pass rate below SLO threshold
- Recovery success below SLO threshold
- Catch-up success below SLO threshold
- Replication lag sustained above threshold
- Onboarding pass rate below 95%

## Implementation notes

- Data can initially come from CI/job outputs and structured logs.
- Move to metrics backend ingestion once runtime metric exporters are finalized.