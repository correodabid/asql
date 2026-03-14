# ASQL Backlog (Agent-executable)

Status note (2026-03-13): this is the active engineering execution backlog.
Use strategy/product snapshot docs for context, but treat this file as the default source for current implementation priority unless a newer doc explicitly supersedes it.

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
- [x] Create release candidate gate checklist (`v1.0.0-rc1`).
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

## Epic AF — PostgreSQL compatibility audit and selective expansion (Phase 8)

Reference inputs:
- `docs/reference/sql-pgwire-compatibility-policy-v1.md`
- `docs/reference/postgres-compatibility-surface-v1.md`
- `docs/ai/11-technical-gap-matrix-vs-postgres.md`

Execution rule:
- Audit before build. If a capability already exists in code/tests, document and regression-cover it before planning net-new implementation work.
- Prefer compatibility work that improves mainstream client/tool interoperability without weakening determinism or ASQL-native transaction semantics.

P0 — reconcile real behavior vs documented surface:
- [x] Audit existing pgwire/session/catalog compatibility against code and tests, then produce a claim-by-claim inventory of what is already implemented.
- [x] Refresh the PostgreSQL compatibility matrix so it includes currently implemented startup/session/catalog shims that are missing or under-specified in docs.
- [x] Refresh the SQL compatibility matrix so it distinguishes `implemented + documented`, `implemented but undocumented`, and `not yet supported` for common app-facing query patterns.
- [x] Add a compatibility evidence map linking each public compatibility claim to one or more regression tests.

P1 — close documentation and regression gaps first:
- [x] Add regression tests for already-implemented compatibility behaviors that are presently relied on implicitly but not claimed explicitly.
- [x] Close the remaining evidence gaps called out in `docs/reference/postgres-compatibility-evidence-v1.md` (CSV `COPY`).
- [x] Publish a concise “mainstream Postgres client/tool flows that work today” guide for `psql`, `pgx`, and GUI tools, including required caveats.
- [x] Document current error/SQLSTATE behavior and identify where ASQL already matches PostgreSQL closely enough to claim compatibility.
- [x] Document the currently supported SQL subset already present in parser/planner/executor but not clearly surfaced in compatibility docs.

P2 — targeted high-return compatibility expansion:
- [x] Expand synthetic catalog/introspection coverage only for additional queries proven necessary by mainstream tool startup/metadata flows.
- [x] Improve `ParameterDescription` / `RowDescription` / bind-format fidelity for common scalar types where mainstream drivers still degrade or fail.
- [x] Tighten SQLSTATE mapping for common compatibility-critical failures (syntax, missing objects, constraint violations, cancellation, transaction state).
- [x] Expand app-facing PostgreSQL-compatible SQL only where it materially reduces migration friction and preserves deterministic replay semantics.

AF-P2 closure note:
- The current documented compatibility wedge has no open evidence-backed mainstream startup/catalog gaps, no open claim-to-test metadata-fidelity gaps for the currently supported common scalar shapes, and no currently justified app-facing SQL expansion that outweighs determinism and surface-area costs.
- Future compatibility expansion remains demand-driven: reopen only when a mainstream client/tool flow demonstrates concrete adoption friction not already covered by the existing docs, tests, and evidence map.

P3 — compatibility operating model:
- [x] Add a repeatable compatibility test pack grouped by client/tool (`psql`, `pgx`, JDBC/GUI baseline) and make it part of release validation.
- [x] Add a triage rubric for deciding whether a reported PostgreSQL-compatibility gap should be solved in docs, protocol/catalog shim, SQL surface, or explicitly rejected as out of scope.
- [x] Establish a rule that new PostgreSQL-compatibility claims are not public until docs, regression tests, and compatibility matrix entries land together.

Acceptance gates (must pass before closing Epic AF)
- [x] Public compatibility docs match real behavior closely enough that “implemented but undocumented” is no longer a recurring source of surprise.
- [x] Each public PostgreSQL compatibility claim is backed by at least one regression test.
- [x] Net-new compatibility work is prioritized by observed client/tool adoption friction, not by parity for parity’s sake.
- [x] ASQL remains explicitly a deterministic engine with a pragmatic PostgreSQL-compatible subset, not a drop-in PostgreSQL replacement.

## Epic AD — Adoption-friction closure from PharmaApp (Phase 8)

Reference inputs:
- `pharmaapp/FRICTION_LOG.md`

P0 — make the core model more adoptable:
- [x] Add first-class documentation and examples that distinguish row-head `LSN` capture from entity-version capture in practical schema design.
- [x] Add stronger pgwire compatibility guidance for driver/query-mode choices, including an explicit recommended path and known-risk path matrix.
- [x] Add guided diagnostics for common temporal-modeling failures (`VERSIONED FOREIGN KEY` resolution, missing entity root, over-broad `CROSS DOMAIN` usage).
- [x] Add one operator/developer-facing signal for temporal-reference and cross-domain adoption patterns, not just raw runtime health.

P1 — reduce repeated integration and schema-evolution work:
- [x] Add reusable Go-side helper patterns for temporal inspection workflows (`current -> history -> AS OF LSN -> explanation`).
- [x] Add a schema-evolution checklist specific to entities and versioned references, including history/replay safety review points.
- [x] Add migration/preflight validation that flags likely historical-semantics changes when entities or versioned references are altered.
- [x] Add a dedicated troubleshooting guide for adoption-time modeling errors with recommended fixes by symptom.
- [x] Add at least one generic Studio or CLI workflow that turns raw temporal primitives into a guided historical explanation flow.

P2 — close the model/runtime feedback loop:
- [x] Expose adoption-oriented metrics or summaries for entity churn, temporal-query usage, and cross-domain breadth.
- [x] Add a lightweight review rubric for deciding when observed friction should become engine work vs docs/SDK/tooling work.

Acceptance gates (must pass before closing Epic AD)
- [x] Teams can choose between row-based and entity-based temporal references without relying on implicit tribal knowledge.
- [x] At least one guided diagnostics path exists for the most common temporal and cross-domain modeling mistakes.
- [x] Schema evolution guidance covers not just SQL validity but historical and replay-visible impact.
- [x] Adoption review can use first-class signals instead of only manual inspection of example apps.

## Epic AE — Decouple ASQL Studio from engine internals

Reference inputs:
- `docs/adr/0003-studio-as-external-product-surface.md`
- `asqlstudio/`

P0 — establish public boundary first:
- [x] Extract the admin request/response contracts used by Studio from `internal/server/httpapi` into a stable public package.
- [x] Extract fixture load/validate/export contracts and helpers from `internal/fixtures` into a stable public package.
- [x] Replace Studio imports of `internal/engine/executor` with public admin API calls for backup/restore and storage-inspection workflows.

P1 — move Studio to product-level layout:
- [x] Move the Studio implementation from `cmd/asqlstudio/` to a root-level `asqlstudio/` folder.
- [x] Leave `cmd/asqlstudio` as a thin bootstrap wrapper or remove it if the root-level entrypoint becomes canonical.
- [x] Update Wails config, embedded assets, workspace tasks, packaging paths, and docs to the new Studio location.

P2 — harden against regression:
- [x] Add checks/tests that fail if Studio imports `internal/engine/*` or `internal/server/*`.
- [x] Stop treating generated Studio build outputs as canonical source artifacts where possible.
- [x] Document Studio explicitly as a public product surface that consumes stable engine interfaces.

Acceptance gates (must pass before closing Epic AE)
- [x] Studio can build and run from a root-level `asqlstudio/` product folder.
- [x] Studio imports no engine-private packages under `internal/engine/*`.
- [x] Studio imports no server-private packages under `internal/server/*`.
- [x] Backup/restore, fixture, and schema workflows are consumed through public contracts or public admin APIs.

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
- `docs/product/production-readiness-roadmap-v1.md`

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
- `docs/product/production-readiness-roadmap-v1.md`

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

- [x] Expand online-safe schema evolution beyond the current baseline.
- [x] Add migration preflight checks as first-class commands.
- [x] Add rollback planning/reporting for schema changes.
- [x] Add audit policy, retention, and export/report workflows.
- [x] Add stronger operational access-control controls for admin APIs.

Acceptance gates (must pass before closing Epic AA)
- [x] Schema evolution supports deterministic rollback and replay parity.
- [x] Audit outputs are usable for external evidence workflows.

## Epic AB — Performance after correctness (Phase 6)

Current evidence already in repo, but not sufficient to close this epic:
- `docs/ai/09-benchmark-baseline.md` captures an internal deterministic engine/WAL baseline.
- `docs/product/performance-benchmark-plan-v1.md` defines the active L0–L4 benchmark ladder, including cluster scenarios.
- `internal/engine/executor/engine_benchmark_test.go` covers commit, concurrent commit, read-as-of-LSN, and replay-to-LSN microbenchmarks.
- `internal/storage/wal/store_benchmark_test.go` covers append, read, and recovery microbenchmarks.
- `internal/engine/executor/engine_query.go` and `internal/engine/executor/engine_scan.go` already contain index-only scan support.

Open gaps before closure:
- Snapshot restart microbenchmarks now exist, but there is no closure-level baseline/improvement decision yet for snapshot load time.
- Initial failover promotion/recovery benchmarks now exist, but there is no closure-level baseline/improvement decision yet for failover recovery time.
- Initial indexed-read and index-only benchmarks now exist, but there is no closure-level baseline/improvement decision yet for indexed read latency.
- No published multi-scenario failover/recovery benchmark suite yet that satisfies the acceptance gates below.
- No measured decision record yet for persisted index/cache architecture or parallel scan evaluation.

Subline status:
- The `Expand index-only scan coverage where benchmarks justify it` item is now evidence-backed for covered simple ordered reads, covered ordered reads with `OFFSET`, covered selective reads with bounded early-stop, and covered composite ordered reads.
- The broader `Benchmark and improve indexed read/query latency` item remains open because not all indexed shapes have been benchmarked or optimized, and the closure decision still needs to be stated at the epic level.
- Replay-throughput now has a stable repeated benchmark sample (~`2.0 ms/op` on the current fixture), but no improvement decision is recorded yet.
- Snapshot-load benchmarking is now better grounded after fixing the restart benchmark harness to clone the WAL/snapshot fixture per iteration, removing one extra deep copy during snapshot materialization, reducing dictionary-string allocation in the binary decoder, and decoding table rows directly into positional slices; repeated longer-benchtime samples now put persisted-snapshot restart at roughly ~`3.91 ms/op` versus ~`3.33–3.41 ms/op` for replay-only, while using only ~`1.12 MB/op` and ~`6.3k allocs/op`, so this item remains open as an optimization target rather than a closed win.
- The old end-to-end persisted-snapshot restart benchmark is a head-snapshot best-case because fixture shutdown flushes snapshots to the current `headLSN`; new tail/cadence sweeps now exist to model non-zero replay tails explicitly and are the right evidence source for deciding snapshot frequency.
- Focused snapshot-load benchmarks now show the main persisted-restart hotspot is snapshot-directory read/decompress/decode/materialization at roughly ~`302–377 µs/op` on the current fixture, while `replayFromSnapshots` state restore is comparatively small (~`60 µs/op`).
- Early cadence spot checks on the current M1 show a snapshot plus ~`500` replayed records beating replay-only at ~`1k` total rows (`12.7 ms` vs `16.2 ms`) and around the medium adaptive anchor of ~`10.5k` total rows (`57.6 ms` vs `95.8 ms`), while the advantage narrows again by ~`50.5k` total rows (`289.9 ms` vs `307.1 ms`), so any closure-level snapshot-cadence decision still needs repeated runs and likely workload-class-specific tuning.
- A new restart workload sweep now exercises `insert_heavy`, `update_heavy`, and `delete_heavy` fixture shapes with the same final-state validation, so cadence analysis no longer assumes append-heavy WAL is representative.
- Repeated `-benchmem -benchtime=100ms -count=3` workload samples on the current M1 now show a clearer split: at the default 500-record anchor, `persisted_snapshot` is worse for `insert_heavy` (~`9.8–10.8 ms` vs ~`8.0–8.4 ms` replay-only) but modestly better for `update_heavy` (~`266–281 ms` vs ~`277–302 ms`) and `delete_heavy` (~`275–282 ms` vs ~`288–292 ms`), while also trimming a small amount of memory/allocations in all three shapes.
- Repeated `-benchmem -benchtime=1x -count=2` workload-cadence samples now make that crossover more concrete: with a 500-record tail, `insert_heavy` is still clearly worse at the small anchor (~`22.1–22.3 ms` vs ~`8.7–15.6 ms`) but strongly better by the medium anchor (~`61.3–62.3 ms` vs ~`95.9–101.5 ms`), while `update_heavy` and `delete_heavy` are already break-even-to-better at the small anchor and widen their wins by the medium anchor (~`4.76–5.00 s` vs ~`4.97–5.24 s` for `update_heavy`, ~`3.31–3.43 s` vs ~`3.50–3.54 s` for `delete_heavy`).
- Heap pressure follows the same direction: by the medium anchor, persisted snapshots cut restart allocation volume sharply for `insert_heavy` (~`35.6 MB` vs ~`62.1–62.4 MB`) and trim it modestly for `update_heavy`/`delete_heavy`, so the eventual policy should be evaluated against both restart latency and allocation pressure, not just wall-clock time.
- The leading policy candidate has now been implemented for persisted checkpoints: the existing volume-based snapshot anchors remain the baseline, but the engine maintains a rolling recent mutation-pressure window and halves the persisted-checkpoint mutation interval when weighted update/delete pressure dominates (`insert=1`, `update=4`, `delete=3`, floor `250` mutations).
- This first implementation intentionally targets persisted checkpoint cadence only, not in-memory snapshot retention: restart wins come from fewer replayed WAL records after disk checkpoints, while hot-path historical queries already prefer in-memory snapshots and should not pay extra write-path cost until there is separate evidence for changing retention.
- Within that snapshot-directory load, binary decode remains the largest measured in-process component, but the direct positional-row decode path pushed it down materially to roughly ~`79–166 µs/op`, ~`186–458 KB/op`, and ~`1.1k–1.7k allocs/op`, ahead of raw file I/O (~`87 µs/op`), zstd decompression (~`74 µs/op`), and snapshot materialization (~`49–57 µs/op`).
- The positional-row decode change clearly improved isolated decode and snapshot-directory load benchmarks, but end-to-end persisted-restart timing is still noisy on the current short benchtime harness and needs repeated confirmation before any stronger claim.
- Delta-chain merge is negligible on the current fixture because it is effectively loading a single persisted snapshot file, so the remaining work in this AB slice is about file read + decode efficiency rather than cross-file snapshot chaining.
- A follow-up inspection with an index-rich snapshot fixture (primary key + hash + btree persisted) raised full snapshot-directory load to about ~`493 µs/op`, but decode still stayed in the same order of magnitude (~`167 µs/op`), so persisted index payload does not appear to create a new dominant hotspot beyond the existing file-read + decode path.
- Removing the extra raw-file accumulation pass in `readAllSnapshotsFromDir()` did not materially shift timings on the current fixture, which further narrows the remaining opportunity to file-read/decompress/decode itself rather than the old two-pass control flow.
- A direct full-snapshot decode experiment via `decodeSnapshotsBinary()` also failed to show a compelling win over the current raw-decode + materialize pipeline on either the base or index-rich fixture, so a larger refactor toward that path is not yet justified by the evidence.
- The persisted-index/cache item is now narrower than it first appeared: a persisted timestamp→LSN side index already exists and is restart-validated, but there is still no evidence-backed need yet for general persisted table indexes or a broader cache layer.
- The parallel-scan item has now been evaluated to a defer decision: there is still no proven workload class that justifies intra-query parallel scan complexity over the current deterministic single-threaded indexed paths.

Latest directional read evidence:
- `BenchmarkEngineReadIndexedRangeBTree` exercised `btree-order` and repeated at ~`353–361 µs/op`.
- `BenchmarkEngineReadIndexOnlyOrderBTree` exercised `btree-index-only` and repeated at ~`34 µs/op`.
- `BenchmarkEngineReadIndexOnlyOrderOffsetBTree` exercised `btree-index-only` and repeated at ~`58–59 µs/op`.
- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree` exercised `btree-index-only` and, after bounded early-stop optimization, repeated at ~`274–275 µs/op`.
- `BenchmarkEngineReadSelectiveNonCoveredBTree` exercised `btree-order` and repeated at ~`406–407 µs/op`.
- `BenchmarkEngineReadCompositeCoveredIndexOnlyBTree` now exercises `btree-index-only` at ~`36 µs/op`.
- `BenchmarkEngineReadCompositeNonCoveredBTree` exercises `btree-order` at ~`76–78 µs/op`.
- Current interpretation: index-only is strongly justified for simple covered ordered reads, remains strong with `OFFSET`, is justified on the selective covered shape after bounded-scan support, and is now also justified for the covered composite ordered-read shape. Further expansion should still be guided by measured query shapes rather than blanket rollout.

- [ ] Benchmark and improve commit batching on realistic workloads.
- [ ] Benchmark and improve replay throughput.
- [ ] Benchmark and improve snapshot load time.
- [ ] Benchmark and improve indexed read/query latency.
- [ ] Benchmark and improve failover recovery time.
- [ ] Evaluate persisted index/cache architecture from measured IO behavior.
- [x] Expand index-only scan coverage where benchmarks justify it.
- [x] Evaluate parallel scans only for proven workload classes.

Acceptance gates (must pass before closing Epic AB)
- [ ] Performance work is benchmark-driven, not assumption-driven.
- [ ] Benchmark suite includes failover and recovery scenarios, not only steady-state throughput.

## Epic AC — Adoption-friction closure from BankApp (Phase 7)

Reference inputs:
- `bankapp/FRICTION_LOG.md`
- `docs/product/asql-adoption-friction-prioritized-backlog-v1.md`

Responsibility boundaries and expectation-setting:
- [x] Add a concise engine-owned vs app-owned note into `docs/getting-started/` and link it from onboarding entry points.
- [x] Add explicit responsibility-boundary callouts to `bankapp/README.md` and at least one getting-started chapter.
- [x] Add a feature-triage rubric for deciding whether a proposed capability belongs in ASQL or the application layer.

Domain and transaction ergonomics:
- [x] Publish a concise domain-modeling guide with examples from at least three different application shapes.
- [x] Add a Go-first reference helper pattern for domain-scoped and cross-domain transaction orchestration.
- [x] Add one supported visibility path for cross-domain overuse (telemetry, logs, CLI diagnostics, or Studio diagnostics).

Temporal and entity ergonomics:
- [x] Add getting-started examples for `current view + historical explanation` workflows combining `FOR HISTORY`, `AS OF LSN`, and helper functions.
- [x] Extend the Go cookbook with generic helper patterns for snapshot lookup, history lookup, and version-to-LSN resolution.
- [x] Publish a concise entity modeling checklist covering `ROOT`, `INCLUDES`, and when not to use entities.

Fixture-first onboarding and compatibility surprise reduction:
- [x] Add a fixture-first onboarding path that starts from a deterministic scenario before service/UI integration.
- [x] Improve fixture validation guidance so common errors point developers toward modeling or ordering fixes.
- [x] Add a concise adoption FAQ for teams coming from SQLite/Postgres/ORM-centric stacks, including pgwire compatibility expectations.

Starter kit ergonomics:
- [x] Define a small general-purpose starter pack for IDs, timestamps, audit metadata shape, transaction helpers, and temporal read helpers.
- [x] Ensure the BankApp example is presented as adoption-learning material subordinate to getting-started, not as a separate learning track.

Acceptance gates (must pass before closing Epic AC)
- [x] Onboarding clearly separates engine-owned and app-owned concerns.
- [x] Teams have one reference path for domain-scoped transaction helpers and temporal query composition.
- [x] Fixture-first onboarding is practical and documented from the main getting-started path.
- [x] Common compatibility and expectation mismatches are documented before teams hit them by trial and error.
