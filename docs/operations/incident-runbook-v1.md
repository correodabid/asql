# ASQL Incident Runbook (v1)

Date: 2026-02-28
Severity model: P0 (service unavailable/data risk), P1 (major degradation), P2 (minor).

## Common first-response checklist

1. Confirm severity and blast radius (domains, nodes, clients).
2. Capture current evidence (logs, latest WAL file, CI status, command outputs).
3. Freeze non-essential changes during mitigation.
4. Announce status in incident channel every 30 minutes (P0/P1).

---

## Scenario 1: Historical query / replay failures

Symptoms:
- `AS OF LSN` or `FOR HISTORY` queries fail unexpectedly.
- historical helper queries return replay/history errors.
- replay parity tests fail.

Likely causes:
- WAL corruption or incompatible/partial history.
- inconsistent historic records from earlier versions.

Actions:
1. Run focused validation test:
   - `go test ./test/integration -run TestReplayToLSNAndTimeTravelQueries -v`
2. Run backup/restore parity test:
   - `go test ./test/integration -run TestBackupWipeRestorePreservesQueryParity -v`
3. If corruption suspected, create backup copy of WAL before further actions.
4. Restore from latest valid backup and re-run parity checks.

Exit criteria:
- Replay/time-travel tests pass.
- Service functionality restored for affected domains.

---

## Scenario 2: Replication catch-up degradation

Symptoms:
- follower lag grows and does not converge.
- catch-up tests fail or divergence alerts fire.

Likely causes:
- out-of-order apply conditions,
- transport interruptions,
- follower WAL append issues.

Actions:
1. Run replication integration checks:
   - `go test ./internal/cluster/replication -v`
2. Verify leader/follower connectivity and stream health.
3. Restart follower apply loop and monitor lag trend.
4. If divergence persists, isolate follower and rebootstrap from leader snapshot/WAL baseline.

Exit criteria:
- catch-up succeeds and lag trend returns to normal.
- no new divergence events for observation window.

---

## Scenario 3: Security/auth failures (pgwire, optional gRPC, token/mTLS)

Symptoms:
- pgwire clients receive unexpected auth or TLS failures,
- optional gRPC/admin clients receive `Unauthenticated` unexpectedly,
- mTLS handshakes fail,
- auth audit failures spike.

Likely causes:
- token mismatch/rotation issue,
- invalid cert chain or expired cert,
- missing client CA configuration.

Actions:
1. Validate current server startup flags (`-auth-token`, `-tls-*`) on affected nodes.
2. Confirm certificate validity period and chain to the configured client CA for each enabled listener.
3. Re-run black-box coverage for the affected surface:
   - `go test ./internal/server/pgwire -v`
   - if the gRPC/admin surface is enabled, `go test ./internal/server/grpc -v`
4. Roll back to the last known good security config if outage is ongoing.

Exit criteria:
- successful authenticated pgwire requests restored.
- optional gRPC/admin requests restored if that surface is in use.
- audit log failure rate returns to baseline.

---

## Post-incident tasks

1. Write incident summary with timeline and root cause.
2. Create backlog items for preventive fixes.
3. Add/adjust regression tests where applicable.