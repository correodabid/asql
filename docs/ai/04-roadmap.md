# ASQL Roadmap (12–18 months)

## Phase 0 — Foundation (Weeks 1–4)
- Initialize Go module and repo skeleton.
- Define core interfaces (ports) and deterministic abstractions.
- Create minimal gRPC service skeleton.
- CI pipeline: fmt, lint, unit tests.

## Phase 1 — Single-node deterministic core (Months 2–4)
- SQL subset parser (CREATE TABLE, INSERT, UPDATE, DELETE, SELECT basic).
- WAL append/recovery.
- Basic B-Tree storage.
- Single-domain transactions.
- Determinism test suite.

**Exit criteria**
- Full restart recovery from WAL.
- Deterministic replay test passes repeatedly.

## Phase 2 — Domain isolation (Months 5–7)
- Domain catalog and namespace isolation.
- `BEGIN DOMAIN` semantics.
- Per-domain storage partitioning.
- Domain-aware telemetry.

**Exit criteria**
- Cross-domain access blocked unless explicit.
- Integration tests for domain isolation.

## Phase 3 — Cross-domain tx + replay tools (Months 8–10)
- `BEGIN CROSS DOMAIN` transaction coordinator.
- Atomic commit protocol for declared domains.
- Replay tooling (`replay to lsn`, log inspection).
- Time-travel query support (`AS OF`).

**Exit criteria**
- Cross-domain deterministic commit/abort behavior validated.
- Time-travel query integration test green.

## Phase 4 — Optional distributed mode (Months 11–14)
- Leader/follower per domain group.
- WAL replication stream over gRPC.
- Follower apply loop with order guarantees.
- Failure/reconnect scenarios.

**Exit criteria**
- Follower catches up and matches leader state hash.
- Deterministic replay remains valid in replicated mode.

## Phase 5 — Hardening + DX (Months 15–18)
- Performance profiling and bottleneck fixes.
- Operational docs and runbooks.
- `asqlctl` CLI for inspect/replay/admin.
- Candidate release and compatibility policy for WAL format.
