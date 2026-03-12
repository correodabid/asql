# ASQL Backlog (Agent-executable)

## How to use this backlog
- Pick top-most unchecked task.
- Implement smallest vertical slice.
- Add tests first/alongside implementation.
- Update docs when behavior changes.

Execution priority for Epics M–P should follow:
- `docs/ai/10-competitive-plan-vs-postgres-mysql.md`

Sprint 2 checkpoint:
- [x] Backup/restore MVP with integrity validation test.

Legend:
- `[ ]` pending
- `[~]` in progress
- `[x]` done

## Epic A — Repository bootstrap
- [x] Create `go.mod` and base folders from blueprint.
- [x] Add `cmd/asqld/main.go` bootstrapping minimal server.
- [x] Add `cmd/asqlctl/main.go` placeholder CLI.
- [x] Add CI workflow for `go test ./...` + `go vet` + `gofmt -w` check.

## Epic B — Deterministic core abstractions
- [x] Define core interfaces: `Clock`, `Entropy`, `LogStore`, `KVStore`, `Telemetry`.
- [x] Implement deterministic `Clock` adapter for tests.
- [x] Implement deterministic serialization utility for WAL payloads.
- [x] Add unit tests proving stable serialization output.

## Epic C — WAL and recovery
- [x] Implement WAL record model and versioning.
- [x] Implement append + fsync strategy abstraction.
- [x] Implement WAL reader with checksum validation.
- [x] Implement startup recovery that rebuilds in-memory state.
- [x] Add corruption handling tests.

## Epic D — SQL parser/planner minimum subset
- [x] Define AST for basic DDL/DML.
- [x] Implement parser for `CREATE TABLE`, `INSERT`, `SELECT` (simple predicates).
- [x] Implement planner for single-table operations.
- [x] Add parser/planner tests with deterministic snapshots.

## Epic E — Execution + single-domain tx
- [x] Build execution pipeline parse -> plan -> execute.
- [x] Implement `BEGIN DOMAIN <name>`.
- [x] Add commit/rollback semantics.
- [x] Emit WAL begin/mutation/commit records.
- [x] Add integration tests for restart + replay.

## Epic F — Domain isolation
- [x] Implement domain catalog metadata.
- [x] Isolate schema per domain.
- [x] Enforce domain access checks at planning time.
- [x] Add tests for forbidden cross-domain access.

## Epic G — Cross-domain transactions
- [x] Implement `BEGIN CROSS DOMAIN <a>, <b>` parser support.
- [x] Implement deterministic domain ordering in coordinator.
- [x] Implement atomic two-phase-like commit (internal).
- [x] Add failure simulation tests for partial failure rollback.

## Epic H — Time-travel + replay API
- [x] Implement query execution `AS OF LSN`.
- [x] Implement `AS OF TIMESTAMP` mapping.
- [x] Add replay-to-lsn engine API.
- [x] Add integration tests for reproducible historical reads.

## Epic I — gRPC API
- [x] Define MVP protobufs.
- [x] Implement `Execute`, `BeginTx`, `CommitTx`, `RollbackTx` handlers.
- [x] Implement replay/time-travel handlers.
- [x] Add black-box gRPC integration tests.

## Epic J — Optional replication
- [x] Implement replication stream RPC.
- [x] Implement follower apply loop with strict `lsn` ordering.
- [x] Add divergence detection (state hash mismatch alert).
- [x] Add integration test leader/follower catch-up.

## Epic K — Production hardening
- [x] Add race-detector CI lane (`go test -race ./...`).
- [x] Add WAL compatibility/version migration tests.
- [x] Add fault-injection tests for crash/recovery loops.
- [x] Publish deterministic benchmark baseline (write/read/replay).

## Epic L — Security baseline
- [x] Add gRPC authn/authz middleware (mTLS and token mode).
	- [x] Token mode middleware (Bearer auth header interceptor).
	- [x] mTLS mode.
- [x] Add structured audit events for tx and admin APIs.
- [x] Add dependency and CVE scanning in CI.
- [x] Add SBOM generation and signed release artifacts.

## Epic M — Developer experience and packaging
- [x] Expand `asqlctl` with operational subcommands.
- [x] Add Docker image and versioned release pipeline.
- [x] Add Go SDK examples and cookbook.
	- [x] Recipe 1: schema init + first write.
	- [x] Recipe 2: cross-domain tx lifecycle.
	- [x] Recipe 3: admin check (time-travel + replay) with strict/best-effort modes.
	- [x] Migration quick path from SQLite usage.
- [x] Add 10-minute getting-started guide.

## Epic N — Beta and operations readiness
- [x] Define SLOs and incident runbooks.
- [x] Add telemetry dashboards for replay/replication health.
- [x] Establish design-partner feedback triage workflow.
- [x] Add upgrade/backward-compat validation in release checklist.

## Epic O — Commercial readiness
- [x] Define pricing/licensing model.
- [x] Add support policy and security disclosure policy docs.
- [x] Publish benchmark and architecture one-pagers.
- [x] Produce migration guide from SQLite/Postgres-lite usage.

## Epic P — Launch readiness
- [ ] Create release candidate gate checklist (`v1.0.0-rc1`).
- [ ] Freeze protocol/WAL compatibility for GA.
- [ ] Finalize docs portal and examples repo.
- [ ] Prepare launch narrative and channels.

## Epic Q — SQL capability expansion (technical return)
- [x] Add `SELECT ... ORDER BY <column> [ASC|DESC] LIMIT <n>` support end-to-end.
- [x] Add multi-column `ORDER BY` support.
- [x] Add inner join support for two-table reads.
- [x] Define deterministic indexing strategy and first index type (hash index for `=` predicates via `CREATE INDEX`).
- [x] Extend indexes with `USING BTREE` and range predicates (`>`, `<`, `>=`, `<=`) for deterministic scans.
- [x] Optimize single-column `ORDER BY` reads using `BTREE` index order fast-path (skip explicit sort when possible).
- [x] Push down `LIMIT` into `BTREE` order fast-path to stop scan early for top-N queries.
- [x] Add bounded `BTREE` scan for aligned `WHERE` + `ORDER BY` to short-circuit range traversal.
- [x] Add `BTREE` prefix optimization for multi-column `ORDER BY` (first key from index, residual sort within key groups).
- [x] Add deterministic scan strategy heuristic (`hash` vs `btree` fast-paths vs `full-scan`) based on query shape and selectivity.
- [x] Add scan strategy counters for observability/benchmarking (`full-scan`, `hash`, `btree-order`, `btree-prefix`).
- [x] Expose scan strategy counters via admin gRPC endpoint (`ScanStrategyStats`) for dashboard consumption.

## Epic R — Correctness and parity floor (8-week)
- [x] Implement deterministic `UPDATE` support end-to-end (parser/planner/executor/WAL/replay).
- [x] Implement deterministic `DELETE` support end-to-end (parser/planner/executor/WAL/replay).
- [x] Add `PRIMARY KEY` and `UNIQUE` constraints with deterministic conflict behavior.
- [x] Implement SQL NULL three-valued logic baseline for WHERE predicates.
- [x] Add minimal deterministic aggregation floor (`COUNT`, `SUM`, `AVG`, `GROUP BY`, `HAVING`).

## Epic S — Concurrency and planning credibility (next 8-week)
- [x] Implement deterministic MVCC snapshot visibility baseline.
- [x] Add deterministic write-write conflict detection at commit.
- [x] Add `FOREIGN KEY` constraints subset with deterministic validation order.
- [x] Add `CHECK` constraints subset with deterministic predicate evaluation.
- [x] Extend expression engine with `AND`/`OR`/`NOT` for `WHERE`/`HAVING`.
- [x] Add arithmetic expression subset for filters/aggregates compatibility.
- [x] Add composite `BTREE` indexes for multi-column query shapes.
- [x] Add lightweight stats + deterministic cost-guided scan strategy selection.
- [x] Add savepoint baseline (`SAVEPOINT` / `ROLLBACK TO`) with deterministic semantics.

Reference plan:
- `docs/ai/11-technical-gap-matrix-vs-postgres.md`

Planning note:
- Priority guidance in `docs/ai/11-technical-gap-matrix-vs-postgres.md` is now aligned to post-Epic U status.

## Epic T — Distributed competitiveness (vs PostgreSQL)
- [x] Add replication `LastLSN` unary RPC for leader visibility.
- [x] Add replication lag helper baseline (`leader_lsn`, `follower_lsn`, `lag`).
- [x] Expose replication lag visibility in ASQL Studio (leader-only and leader/follower modes).
- [x] Add replica read path for read-only queries.
- [x] Add deterministic lag-aware routing policy (staleness threshold, fallback to leader).
- [x] Add deterministic test suite for routing decisions under lag transitions.
- [x] Emit routing and staleness telemetry counters for operators.
- [x] Document consistency modes and stale-read contract for clients.

## Epic U — Deterministic HA baseline (2-week execution)

Week 1 — control-plane invariants
- [x] Define domain-group leadership model and invariants (`single active leader`, `monotonic term`, `fencing token`).
- [x] Implement leader lease heartbeat with deterministic timeout abstraction (`Clock`) and explicit lease expiry state.
- [x] Add follower promotion preconditions (`up-to-date LSN`, `term check`, `no stale lease`) before writable role transition.
- [x] Add admin visibility endpoint(s) for role/term/lease state per domain-group.

Week 2 — failover behavior and correctness proofs
- [x] Implement deterministic failover flow (`leader down` -> `candidate elected` -> `promoted leader`) with serialized state transitions.
- [x] Gate writes with fencing token check to prevent split-brain under delayed/stale leaders.
- [x] Add failure simulation integration tests: leader crash, delayed heartbeat, dual-candidate contention, stale leader recovery.
- [x] Add replay/state-hash continuity tests across promotion to prove deterministic equivalence.

Acceptance gates (must pass before closing Epic U)
- [x] Repeated failover simulations produce identical winner/term sequence for same seeded timeline.
- [x] No split-brain detected in adversarial tests (concurrent write attempts rejected for stale token).
- [x] Post-failover replay hash equals baseline hash for equivalent WAL inputs.
- [x] Observability covers election/failover events and fencing rejections.

## Epic V — Post-Epic U competitiveness execution (8-week)

Reference plan:
- `docs/ai/11-technical-gap-matrix-vs-postgres.md`

Sprint V1 (Weeks 1–2) — replica-read policy promotion to reusable API/service
- [x] Promote lag-aware read routing policy from Studio-only flow into shared server/API path.
- [x] Expose consistency-window metadata (`mode`, `leader_lsn`, `follower_lsn`, `lag`, `fallback_reason`) in API responses.
- [x] Add deterministic routing tests for repeated seeded lag timelines (same input => same route decision).
- [x] Emit operator telemetry counters for route decisions and fallback causes from reusable service path.

Sprint V2 (Weeks 3–4) — optimizer depth + explainability
- [x] Add deterministic join strategy expansion for supported multi-table shapes.
- [x] Improve index candidate selection when multiple deterministic access paths are valid.
- [x] Add `EXPLAIN`/plan diagnostics output with deterministic plan shape serialization.
- [x] Add planner determinism snapshots to prevent non-deterministic plan drift.

Sprint V3 (Weeks 5–6) — PostgreSQL protocol compatibility wedge
- [x] Implement narrow PostgreSQL wire/protocol compatibility spike for high-value SQL subset.
- [x] Validate one mainstream Postgres client/tool roundtrip against ASQL.
- [x] Add compatibility mode tests proving deterministic behavior is preserved.
- [x] Publish explicit supported/unsupported compatibility surface matrix.

Sprint V4 (Weeks 7–8) — schema evolution + migration ergonomics baseline
- [x] Add online-safe schema evolution primitives for practical rollout workflows.
- [x] Add deterministic migration guardrails/checks (preflight + rollback safety validation).
- [x] Extend migration runbook for SQLite/Postgres-lite with rollback and verification paths.
- [x] Add integration tests for schema evolution + migration parity under replay/restart.

Acceptance gates (must pass before closing Epic V)
- [x] Reusable replica-read API path is deterministic and production-consumable (not Studio-only).
- [x] Planner explainability exists and deterministic plan snapshots remain stable across runs.
- [x] One external Postgres-compatible client/tool works end-to-end in compatibility spike scope.
- [x] Schema evolution/migration workflows are validated with deterministic rollback and replay parity tests.

## Epic W — Production-readiness execution kickoff (Phase 1)

Reference plan:
- `docs/production-readiness-roadmap-v1.md`

Development-stage rule:
- Backward compatibility is not a current constraint.
- Prefer removing transitional paths over preserving them.
- Preserve architectural correctness, not intermediate formats.

Cluster convergence and control-plane simplification:
- [x] Declare pgwire + Raft as the only production cluster runtime in docs and code comments.
- [x] Identify every remaining heartbeat-led cluster control path outside the pgwire + Raft runtime.
- [x] Remove or hard-disable heartbeat-led writable cluster mode from non-production paths.
- [x] Unify leader discovery and write-acceptance checks behind a single production authority.
- [x] Eliminate duplicated cluster role semantics where heartbeat and Raft can disagree.

Replication and failover hardening:
- [x] Verify that all cluster-mode write commits flow through the Raft quorum path only.
- [x] Add deterministic tests for leader crash during sustained write load.
- [x] Add deterministic tests for follower lag, catch-up, and resumable replication.
- [x] Add deterministic tests for one-follower partition without quorum loss.
- [x] Add deterministic tests for stale leader recovery after partition healing.
- [x] Add deterministic tests for rolling node restarts under sustained load.

Control-plane documentation:
- [x] Write a production cluster control-plane note describing leader election, quorum commit, replication, fencing, and failover.
- [x] Document which cluster paths are production, legacy, or transitional.

Acceptance gates (must pass before closing Epic W)
- [x] No ambiguous writable cluster path remains outside pgwire + Raft.
- [x] Same seeded failure timeline yields identical leadership and state outcomes.
- [x] All cluster-mode writes are demonstrably quorum-protected.
- [x] Production cluster control-plane documentation matches actual runtime behavior.

## Epic X — Recovery and historical correctness (Phase 2)

Reference plan:
- `docs/production-readiness-roadmap-v1.md`

Time-travel indexing:
- [x] Add a persisted timestamp -> LSN lookup structure.
- [x] Ensure timestamp lookup survives restart/replay and large WAL histories.
- [x] Add deterministic tests for timestamp lookup correctness across snapshots and replay.

Point-in-time recovery primitives:
- [x] Define base-backup metadata format.
- [x] Add WAL segment catalog for restore workflows.
- [x] Implement restore-to-LSN workflow.
- [x] Implement restore-to-timestamp workflow.
- [x] Add operator-facing recovery commands to `asqlctl`.

Durability and corruption handling:
- [x] Add torn-write simulation tests.
- [x] Add checksum failure and corruption drill tests.
- [x] Add snapshot/WAL mismatch detection and failure handling.
- [x] Add replay-from-backup validation tests.

Acceptance gates (must pass before closing Epic X)
- [x] Restore to exact LSN/timestamp is documented and test-covered.
- [x] Recovery runbook is executable without internal knowledge.
- [x] Corruption paths fail closed and are operator-visible.

## Epic Y — Observability and operability (Phase 3)

Metrics and health:
- [x] Add native Prometheus metrics endpoint.
- [x] Expose commit latency, fsync latency/errors, replay duration, snapshot duration/size, replication lag, and failover metrics.
- [x] Expose audit backlog/errors and stale-read routing decision metrics.
- [x] Add structured readiness and liveness semantics.

Operator surfaces:
- [x] Expose leader/follower state and last durable LSN in admin surfaces.
- [x] Expose failover history and snapshot catalog in admin surfaces.
- [x] Expose WAL retention state in admin surfaces.
- [x] Extend Studio and CLI flows for production operator diagnostics.

Acceptance gates (must pass before closing Epic Y)
- [x] A production operator can determine health, safety, lag, and degradation from first-class signals.
- [x] SLOs map directly to emitted metrics.

## Epic Z — Compatibility and operator UX (Phase 4)

PostgreSQL interoperability:
- [x] Refresh PostgreSQL compatibility matrix so docs match real behavior.
- [x] Harden extended query protocol behavior with conformance-style tests.
- [x] Add protocol cancellation.
- [x] Add narrow `COPY` support for high-value ingest/export flows.
- [x] Expand auth/TLS and catalog compatibility where it materially improves tool interoperability.
	- [x] Add narrow pgwire password authentication using the configured shared auth token.
	- [x] Add targeted catalog/query interception for common `current_setting(...)` and startup-introspection flows used by `psql`/GUI tools.
		- [x] `current_setting('param')` — maps ~22 commonly queried GUC params to sensible defaults.
		- [x] `set_config(name, value, is_local)` — no-op that echoes the value back.
		- [x] `pg_is_in_recovery()` — returns leader/follower state.
		- [x] `pg_backend_pid()` — synthetic PID.
		- [x] `inet_server_addr()` / `inet_server_port()`.
		- [x] `pg_encoding_to_char()`.
		- [x] `obj_description` / `col_description` / `shobj_description` — return empty.
		- [x] `has_schema_privilege` / `has_table_privilege` / `has_database_privilege` — return true.
		- [x] `pg_catalog.pg_settings` — synthetic table with GUC rows.
		- [x] `pg_catalog.pg_database` — single "asql" database entry.
		- [x] `SHOW search_path` and 15+ additional SHOW params.
		- [x] Simple query path now routes through `interceptCatalog` (was extended-query only).
	- [x] Reassess whether any minimal TLS negotiation/auth surface is needed beyond explicit `SSLRequest -> N` behavior for mainstream tools.
		- Conclusion: no changes needed. All mainstream tools default to `sslmode=prefer`, which gracefully falls back to plaintext when server declines TLS.
		- `sslmode=prefer`, `sslmode=allow`, `sslmode=disable` validated with integration tests (`TestSSLModePreferFallback`).
		- TLS transport deferred to Phase 6+ (production-hardening concern, not tool-interop blocker).
		- Fixed `set_config()` to preserve value case (was extracting from lowercased string).

CLI/operator ergonomics:
- [x] Add interactive `asqlctl shell` mode.
- [x] Support history, transactions, explain, replay, and cluster-admin workflows in the shell.
	- [x] Pgwire-connected REPL with multi-line input and semicolon-terminated statements.
	- [x] Meta-commands: `\q`, `\?`, `\dt`, `\d <table>`, `\l`/`\domains`, `\timing`, `\cluster`, `\conninfo`, `\history`.
	- [x] Transaction-aware prompt (`=>` vs `=#`, domain display).
	- [x] Tabular result formatting with column-width auto-sizing.
	- [x] Persistent command history at `~/.asql_history`.
	- [x] Cluster-admin: `\cluster` shows node role, leader, peers, raft term.

Acceptance gates (must pass before closing Epic Z)
- [x] 2-3 mainstream Postgres client/tool flows work within the documented compatibility surface.
	- psql startup flow: validated (`TestMainstreamToolStartupFlows/psql_startup`).
	- DBeaver/DataGrip startup flow: validated (`TestMainstreamToolStartupFlows/dbeaver_datagrip_startup`).
	- pgx Go SDK driver flow: validated (`TestMainstreamToolStartupFlows/pgx_go_driver_startup`).
	- End-to-end data workflow (CREATE/INSERT/UPDATE/DELETE/SELECT): validated.
- [x] Daily operator workflows can be executed without ad hoc scripts.
	- `asqlctl shell` provides interactive SQL access with meta-commands, history, and cluster admin.
	- Admin HTTP endpoints expose health, metrics, leadership, WAL retention, and failover history.
	- `asqlctl` CLI covers backup, restore, replay, time-travel operations.

## Epic AA — Schema operations and compliance depth (Phase 5)

- [ ] Expand online-safe schema evolution beyond the current baseline.
- [ ] Add migration preflight checks as first-class commands.
- [ ] Add rollback planning/reporting for schema changes.
- [ ] Add audit policy, retention, and export/report workflows.
- [ ] Add stronger operational access-control controls for admin APIs.

Acceptance gates (must pass before closing Epic AA)
- [ ] Schema evolution supports deterministic rollback and replay parity.
- [ ] Audit outputs are usable for external evidence workflows.

## Epic AB — Performance after correctness (Phase 6)

- [ ] Benchmark and improve commit batching on realistic workloads.
- [ ] Benchmark and improve replay throughput.
- [ ] Benchmark and improve snapshot load time.
- [ ] Benchmark and improve indexed read/query latency.
- [ ] Benchmark and improve failover recovery time.
- [ ] Evaluate persisted index/cache architecture from measured IO behavior.
- [ ] Expand index-only scan coverage where benchmarks justify it.
- [ ] Evaluate parallel scans only for proven workload classes.

Acceptance gates (must pass before closing Epic AB)
- [ ] Performance work is benchmark-driven, not assumption-driven.
- [ ] Benchmark suite includes failover and recovery scenarios, not only steady-state throughput.
