# ASQL Production-Readiness Review and Roadmap (v1)

Date: 2026-03-11

## Executive summary

This document supersedes the older production proposal and should be treated as the current source of truth.

Most importantly:

- ASQL is **not** on a "mesh only" consensus prototype anymore in the main `asqld` runtime.
- The primary server path already wires **Raft-backed quorum commits** into the engine.
- MVCC, WAL durability checks, deterministic replay, cross-domain transactions, time-travel, audit logging, `EXPLAIN`, and backup/restore MVP already exist.

The real gap is no longer "build the foundations from scratch". The next job is to **turn the existing deterministic + replicated core into an operationally boring system**: unify cluster paths, harden failover under adversarial conditions, close protocol/ops gaps, and add production-grade observability, recovery, and release gates.

Just as important, ASQL should optimize for the niche where it can actually win:

- strict domain isolation,
- immutable auditability,
- deterministic replay,
- offline/edge-first operation with eventual sync.

Trying to be "better than Postgres at everything" is not a realistic near-term target. Being production-ready and clearly superior in that niche is.

One explicit planning assumption for the current stage:

- ASQL is still in active development, so **backward compatibility is not a current constraint**.
- WAL format, snapshots, admin APIs, and protocol details may change whenever that simplifies the path to a sounder architecture.
- Compatibility should be treated as a **later stabilization concern**, not as a blocker for current design corrections.

---

## Review of the retired proposal assumptions

### 1) "Reemplazar mesh por Raft"

**Assessment:** outdated for the main server path; partially true for legacy code paths.

Evidence:
- `cmd/asqld` boots the pgwire server, not the standalone gRPC server.
- The pgwire server creates a `RaftNode`, registers the Raft gRPC service, and routes engine commits through a Raft committer.
- The engine commit path explicitly supports quorum-backed writes.

What is still true:
- There is still a **legacy heartbeat-based leadership path** in the gRPC server abstraction and some compatibility glue around heartbeat/failover. That should be retired or strictly demoted to test/legacy mode so there is one production cluster path.

Conclusion:
- The correct statement today is: **Raft is already integrated into the main runtime, but production-readiness still requires cluster-path unification and stronger failure testing.**

### 2) "MVCC correcto con snapshot isolation"

**Assessment:** mostly outdated.

Evidence:
- Backlog marks deterministic MVCC snapshot visibility and write-write conflict detection as complete.
- Tests cover consistent snapshots under concurrent reads and write-write conflict detection.

Conclusion:
- Snapshot-isolation baseline exists.
- Remaining work is not "invent MVCC" but **stress, long-running transaction, and performance validation**.

### 3) "WAL fsync + checksum por record"

**Assessment:** outdated.

Evidence:
- WAL store uses sync strategy with fsync.
- WAL binary format includes CRC32C per record.
- Recovery/backup tests validate checksum handling.

Conclusion:
- This foundation exists.
- Remaining work is around **operational recovery workflows, corruption drills, PITR, and durability verification under crash matrices**.

### 4) "Recovery determinista probada"

**Assessment:** outdated.

Evidence:
- Restart/replay integration tests exist.
- Failover continuity tests compare replay/state hash across promotion.
- Backlog and docs call out fault-injection and replay continuity coverage.

Conclusion:
- Deterministic replay is already a real capability.
- Remaining work is **broader fault models, soak tests, and release gating**.

### 5) "Cross-domain transactions ... spec'd pero no implementadas"

**Assessment:** outdated.

Evidence:
- Backlog marks cross-domain transactions complete.
- Internal two-phase-like atomic commit is implemented.
- Failure rollback tests exist.

Conclusion:
- Cross-domain transactions already exist as an internal deterministic coordinator protocol.
- Future work is **observability, UX, and stronger recovery semantics across replicated clusters**.

### 6) "Time-travel queries con índice sobre LSN→timestamp"

**Assessment:** now true in a narrow, targeted form.

Evidence:
- Time-travel by LSN and logical timestamp exists.
- Snapshots accelerate historical reads.
- `LSNForTimestamp` now uses a persisted timestamp→LSN side index (`timestamp-lsn.idx`) when snapshot persistence is enabled.
- Restart-path tests verify timestamp lookups can avoid extra WAL reads after restart and catch-up.

Conclusion:
- The feature exists and already has a justified persisted auxiliary index for timestamp lookup.
- Broader persisted index/cache work should still follow measured IO behavior, not assumption.

### 7) "Audit trail nativo por dominio"

**Assessment:** outdated.

Evidence:
- Persistent audit store exists.
- WAL format supports audit records.
- Engine commit path flushes audit entries.
- `FOR HISTORY` can use the audit store.

Conclusion:
- Native audit is already present.
- The next step is **policy, retention, export, and compliance UX**, not basic implementation.

### 8) "PostgreSQL wire compatibility real ... prepared statements, cancellation, COPY"

**Assessment:** partially true, but the document understates current progress.

Evidence:
- Extended query protocol support exists (`Parse` / `Bind` / `Describe` / `Execute` pipeline).
- There is already a supported compatibility wedge and pgx-based interoperability work.
- Protocol cancellation support exists and is regression-covered.
- Narrow text/CSV `COPY` support exists and is regression-covered.

Conclusion:
- The right statement today is: **pgwire compatibility is beyond a simple spike, but still far from a drop-in Postgres replacement.**
- Main gaps are now auth/TLS breadth, catalog compatibility breadth, and continued conformance hardening around the documented subset.

### 9) "CLI tipo psql funcional"

**Assessment:** mostly true as a gap.

Evidence:
- `asqlctl` exists and is useful for RPC-style operations.
- It is not yet an interactive psql-like shell.

Conclusion:
- Keep this as a production-readiness item.

### 10) "Métricas Prometheus nativas"

**Assessment:** outdated.

Evidence:
- Admin HTTP already exposes a native `/metrics` endpoint in the main pgwire runtime.
- The exported metrics already cover health/readiness, WAL durability, commit/fsync latency, replay/snapshot timing, replication lag, and failover signals.
- Admin HTTP tests validate Prometheus output fragments.

Conclusion:
- Native Prometheus export already exists.
- The remaining work is operational hardening: keep dashboard/SLO mappings current, make `/metrics` part of the release-candidate gate, and ensure operator docs remain aligned with emitted metrics.

### 11) "Query explain plan legible"

**Assessment:** outdated.

Evidence:
- `EXPLAIN` exists in the engine plus gRPC/HTTP APIs.
- Tests verify deterministic plan shape.

Conclusion:
- Explainability exists; what remains is **operator polish**, not baseline support.

### 12) "Backup/restore incremental — WAL-based, point-in-time recovery"

**Assessment:** partially true.

Evidence:
- Backup/restore MVP exists with integrity validation and parity tests.
- I did not find full PITR or incremental backup orchestration.

Conclusion:
- The MVP exists, but **incremental backup catalogs, restore tooling, and PITR** are still roadmap items.

### 13) "B-tree con buffer pool"

**Assessment:** not the best current framing.

Evidence:
- The on-disk `internal/storage/btree` layer is not yet the active center of the runtime in the way this older analysis assumed.
- The engine already has deterministic index strategies and even index-only scans.

Conclusion:
- The real performance question is broader: **memory model, persisted index strategy, snapshot/WAL replay cost, and hot-path IO behavior**.
- A classic page-cache/buffer-pool project may still matter later, but it should follow profiling, not assumption.

### 14) "Parallel query execution" / "Connection pooling integrado"

**Assessment:** mixed.

Evidence:
- I found no evidence of parallel scan/query execution in the engine.
- There is topology-aware connection pooling in the SDK/client side, but not a built-in server-side pooler equivalent to PgBouncer.

Conclusion:
- Parallel query remains future work.
- Pooling should be framed as **client/sdk and deployment ergonomics**, not a first-order correctness blocker.

---

## Current status by area

## Strategic positioning

ASQL's most credible production wedge is not general-purpose database parity.

It is this combination:

- deterministic replay as a first-class contract,
- strict domain isolation inside one engine,
- immutable audit trail and historical reads,
- practical operation in edge/offline-first or compliance-heavy systems.

That means roadmap decisions should be filtered by one question:

**Does this make ASQL safer, more operable, or more adoptable in that wedge?**

If not, it should usually rank below durability, failover, recovery, observability, and migration ergonomics.

### Already credible today

- Deterministic WAL + replay
- Per-record checksum + fsync strategy
- Domain isolation
- Cross-domain transactions
- MVCC snapshot baseline
- Time-travel reads
- Persistent audit log
- Explain/plan diagnostics
- Raft-backed quorum commit in main cluster runtime
- Failover simulation and state-hash continuity tests
- Backup/restore MVP with integrity checks

### Partially complete / needs hardening

- Cluster architecture unification (Raft vs legacy heartbeat control path)
- PostgreSQL compatibility breadth
- Time-travel timestamp index
- Online schema evolution breadth
- Operational telemetry/export
- Backup/restore beyond MVP
- Security/compliance runtime depth

### Still materially missing

- PITR / incremental backup workflow
- Interactive admin shell / psql-like CLI
- Formal release/chaos gates for GA
- Large-scale soak and adversarial fault testing
- broader auth/TLS and catalog compatibility for mainstream PostgreSQL tooling

---

## Production-readiness roadmap

## Development-stage rule

Until ASQL enters a stabilization phase, the roadmap should prefer:

- simpler internals over compatibility shims,
- safer rewrites over preserving intermediate formats,
- clearer operational semantics over upgrade continuity.

That means it is acceptable, and often preferable, to:

- break WAL/snapshot/internal metadata formats,
- change admin or cluster APIs,
- remove transitional code paths,
- rewrite partial implementations that would otherwise calcify into long-term debt.

The only thing that should be preserved right now is **architectural correctness**, not compatibility.

## Phase 1 — Make clustering boring (0–6 weeks)

Goal: one production cluster path, one failure model, one set of operational guarantees.

### Deliverables
- Declare the pgwire + Raft path as the only production cluster runtime.
- Remove or explicitly mark the heartbeat-led gRPC cluster mode as legacy/non-production.
- Unify leader discovery, failover, replication, and write fencing around one authority.
- Add a clear architecture note describing the production cluster control plane.
- Add release-blocking multi-node tests for:
  - leader crash during write burst,
  - follower lag and catch-up,
  - partition of one follower,
  - stale leader recovery,
  - restart of each node during sustained load.

### Exit criteria
- No ambiguous production path remains.
- Same seeded failure timeline produces identical leadership and state outcomes.
- Every write in cluster mode is demonstrably quorum-protected.

## Phase 2 — Recovery, durability, and historical correctness (4–10 weeks)

Goal: prove that ASQL survives bad days, not just happy-path restarts.

### Deliverables
- Persist a dedicated timestamp→LSN index for time-travel lookup.
- Add PITR primitives:
  - base backup metadata,
  - WAL segment catalog,
  - restore-to-LSN / restore-to-logical-timestamp workflow.
- Keep these storage/recovery formats free to change until stabilization; prioritize correctness and simpler internals over migration compatibility.
- Expand corruption and crash testing:
  - torn write simulation,
  - checksum failure drills,
  - snapshot/WAL mismatch handling,
  - replay-from-backup validation.
- Add an operator recovery command set to `asqlctl`.

### Exit criteria
- Restore to exact LSN or logical timestamp is documented and test-covered.
- Recovery runbook is executable without internal knowledge.
- Corruption paths fail closed and are observable.

## Phase 3 — Observability and operability (8–14 weeks)

Goal: make ASQL monitorable, debuggable, and supportable in real deployments.

### Deliverables
- Harden and document the existing Prometheus/admin observability surface.
- Stabilize metric families for:
  - commit latency,
  - Raft term/election/failover,
  - replication lag,
  - replay duration,
  - snapshot duration/size,
  - WAL fsync latency/errors,
  - audit backlog/errors,
  - routing decisions and stale-read fallbacks.
- Harden readiness/liveness and structured health endpoints.
- Better admin surfaces in Studio and CLI for:
  - leader/follower state,
  - last durable LSN,
  - failover history,
  - snapshot catalog,
  - WAL retention status.

### Exit criteria
- A production operator can answer "is it safe, healthy, lagging, or degraded?" in under 5 minutes.
- SLOs map directly to emitted metrics.

## Phase 4 — Compatibility and developer adoption (10–18 weeks)

Goal: remove friction for real applications evaluating ASQL.

### Deliverables
- Refresh PostgreSQL compatibility matrix so docs match reality.
- Harden extended query protocol behavior and add conformance tests.
- Keep protocol cancellation and narrow `COPY` support regression-covered as the compatibility wedge evolves.
- Expand auth/TLS and catalog compatibility where it most improves tool interoperability.
- Extend the interactive `asqlctl shell` with stronger explain, replay, and cluster-admin workflows.

Note:
- This phase is about external interoperability, not backward compatibility of ASQL internals.
- It is acceptable to break internal protocol/storage details while improving the external operator/developer surface.

### Exit criteria
- At least 2–3 mainstream Postgres client/tool flows work end-to-end inside the documented compatibility surface.
- CLI is usable for daily operator workflows without ad hoc scripts.

## Phase 5 — Safe schema evolution and compliance depth (14–22 weeks)

Goal: support real change management in regulated or high-availability systems.

### Deliverables
- Expand online-safe schema evolution beyond the current baseline.
- Add migration preflight checks and rollback plans as first-class commands.
- Add audit policies, retention controls, export/report workflows, and compliance-friendly evidence packs.
- Add richer access-control controls for operational/admin APIs.

### Exit criteria
- Teams can evolve schemas with deterministic rollback and replay parity.
- Audit outputs are usable for external evidence workflows.

## Phase 6 — Performance after correctness (18–28 weeks)

Goal: optimize only after the runtime and operator model are stable.

### Deliverables
- Profile-led work on real bottlenecks:
  - commit batching,
  - replay throughput,
  - snapshot load time,
  - query latency on indexed reads,
  - failover recovery time.
- Evaluate persisted index/cache architecture based on measured IO behavior.
- Expand index-only scan coverage where valuable.
- Consider parallel scans only for proven workload classes.
- Consider built-in pooling only if client/sdk pooling is insufficient in real deployments.

### Exit criteria
- Performance work is benchmark-driven, not assumption-driven.
- Published benchmark suite includes failover and recovery scenarios, not only steady-state throughput.

---

## Recommended priority order

If the goal is "production ready", the order should be:

1. **Unify and harden clustering around Raft**
2. **Finish recovery/PITR and durability workflows**
3. **Add first-class observability**
4. **Close protocol + CLI adoption gaps**
5. **Expand schema/compliance operations**
6. **Only then chase larger performance architecture bets**

---

## Planning realism

Production readiness here should be understood as **credible, supportable deployment readiness for ASQL's target niche**, not universal database parity.

Practical implication:

- Near-term realistic goal: become production-ready for compliance-heavy, domain-isolated, replay/audit-sensitive workloads.
- Near-term unrealistic goal: match PostgreSQL breadth across the full SQL, tooling, ecosystem, and operational surface.

It should also be understood as:

- **production readiness after architectural convergence**, not while preserving every intermediate implementation decision made during development.

A reasonable ambition is to become clearly production-credible in the ASQL niche over the next focused execution window, while treating broad parity as a multi-stage, longer-term effort.

---

## Suggested GA gate

ASQL should not call itself production-ready until all of these are true:

- quorum-backed writes are the only supported cluster write path,
- failover tests pass repeatedly under seeded and adversarial timelines,
- restore-to-LSN and restore-to-logical-timestamp are documented and tested,
- Prometheus metrics and operational dashboards exist,
- compatibility surface is explicit and tested,
- backup + rollback procedures are release-blocking,
- a 24h+ soak suite passes without replay divergence or leadership anomalies.

Pre-GA note:
- backward compatibility should only become a hard requirement once the cluster/storage/runtime model is considered stable enough to freeze.

---

## Bottom line

Production readiness is now mostly about **irreversible operational decisions**, not random feature accumulation.

ASQL already has more foundation than the older proposal gave it credit for. The next step is not "replace the prototype"; it is **finish the transition from credible deterministic engine to boring, observable, recoverable database system** in the niche where ASQL can genuinely win.
