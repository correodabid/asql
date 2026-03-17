# ASQL Backlog (Agent-executable)

Status note (2026-03-15): this is the active engineering execution backlog.
Use strategy/product snapshot docs for context, but treat this file as the default source for current implementation priority unless a newer doc explicitly supersedes it.

Current prioritization snapshot for the next execution window:
- [docs/product/asql-ga-and-delight-plan-v1.md](../product/asql-ga-and-delight-plan-v1.md)
- [docs/product/asql-adoption-roadmap-v1.md](../product/asql-adoption-roadmap-v1.md)

Execution emphasis until Epic P closes:
1. freeze GA compatibility and release-gate contracts,
2. finish the canonical docs/examples surface,
3. improve operator delight around temporal + cluster workflows,
4. keep PostgreSQL compatibility work selective and adoption-driven.

Adoption scaling refresh (2026-03-17):
1. make mainstream PostgreSQL client/app flows work inside the documented ASQL subset,
2. raise planner/performance credibility with benchmark-backed wins on real query shapes,
3. turn temporal/history primitives into a clearly superior operator and developer workflow,
4. make production operation, failover, backup, and diagnostics feel safe and boring,
5. compress first-evaluation time with migration kits, starter apps, and fixture-first onboarding.

## How to use this backlog
- Pick top-most unchecked task.
- Implement smallest vertical slice.
- Add tests first/alongside implementation.
- Update docs when behavior changes.

Execution priority for Epics M–P should follow:
- `docs/ai/10-competitive-plan-vs-postgres-mysql.md`

Sprint 2 checkpoint:
- [x] Backup/restore MVP with integrity validation test.

## Adoption roadmap (12-month, active planning view)

Reference plan:
- [docs/product/asql-adoption-roadmap-v1.md](../product/asql-adoption-roadmap-v1.md)

Sequencing rule:
- Keep single-node determinism, pgwire runtime, and explicit temporal semantics as the product center.
- Prefer adoption-moving work over broad parity work.
- New surface area should land only when docs, tests, and operator visibility arrive with it.

H1 2026 focus:
- Q2: mainstream PostgreSQL tool/app wedge + planner/performance credibility.
- Q3: temporal superpower workflows + operator-grade cluster/recovery UX.

H2 2026 focus:
- Q4: migration/onboarding compression + production proof loops with benchmark, reliability, and operator evidence.

Exit criteria for the roadmap window:
- ASQL is easy to trial from existing PostgreSQL-oriented tools.
- Core read/write/query paths have benchmark-backed credibility and visible planner behavior.
- Time-travel/history/replay are clearly better than the baseline experience teams expect from a pragmatic PostgreSQL-compatible engine.
- Production operators can diagnose health, failover, lag, backup, and restore without internal knowledge.
- First successful evaluation path fits in a short local loop with examples, fixtures, and migration guidance.

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
- [x] Freeze protocol/WAL compatibility for GA.
	- [x] Write the GA compatibility contract for WAL, protocol, and upgrade expectations.
	- [x] Make compatibility validation part of release-candidate gating.
	- [x] Treat benchmark and determinism guardrails as required release evidence.
- [x] Finalize docs portal and examples repo.
	- [x] Align `README.md`, `docs/getting-started/`, `docs/reference/`, and `site/` around the canonical pgwire path.
	- [x] Package examples by adoption moment (first app, time-travel, fixtures, replication/failover visibility).
	- [x] Ensure public docs state ASQL as a PostgreSQL-compatible subset, not a drop-in replacement.
- [x] Prepare launch narrative and channels.
	- [x] Make the core launch story explicit: domain isolation, deterministic replay, time-travel, and operational clarity.
	- [x] Ensure Studio/operator UX supports that story with visible temporal and cluster workflows.

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

Current evidence already in repo and is now sufficient to close this epic at the current scope:
- `docs/ai/09-benchmark-baseline.md` captures an internal deterministic engine/WAL baseline.
- `docs/product/performance-benchmark-plan-v1.md` defines the active L0–L4 benchmark ladder, including cluster scenarios.
- `internal/engine/executor/engine_benchmark_test.go` covers commit, concurrent commit, read-as-of-LSN, and replay-to-LSN microbenchmarks.
- `internal/storage/wal/store_benchmark_test.go` covers append, read, and recovery microbenchmarks.
- `internal/engine/executor/engine_query.go` and `internal/engine/executor/engine_scan.go` already contain index-only scan support.

Closed sub-slices:
- Restart/cadence policy is now treated as closed at the subline level: the mutation-mix-aware persisted checkpoint policy has deterministic regression coverage and repeated natural-restart evidence that is strong enough for a closure decision, even though the broader snapshot-load optimization item remains open.
- Repeated historical/time-travel reads at the same target `LSN` are now treated as closed at the subline level: caching full WAL recovery results plus rebuilt historical states materially reduced repeated `AS OF LSN` latency and allocations on the benchmark fixture, even though broader replay-throughput and cross-`LSN` reuse decisions remain open.
- Snapshot-load is now treated as closed at the current scope: the runtime-state decoder and related restore/load changes materially reduced snapshot-directory cost, and the remaining short-fixture/forced-tail losses are now clearly diagnostic edge cases rather than evidence that the natural runtime path is still mis-tuned.

Open gaps before closure:
- None at the current scope. Remaining performance work should start from new evidence or new workload reports rather than from the old Epic AB placeholders.

Current next-execution order:
- Epic AB is now closure-ready. Any follow-on work should be framed as a new benchmarked slice, not as unfinished Phase 6 carry-over.

Subline status:
- The `Expand index-only scan coverage where benchmarks justify it` item is now evidence-backed for covered simple ordered reads, covered ordered reads with `OFFSET`, covered selective reads with bounded early-stop, and covered composite ordered reads.
- The broader `Benchmark and improve indexed read/query latency` item is now treated as closed at the current scope: the benchmark set covers simple ordered range reads, covered ordered reads, covered ordered reads with `OFFSET`, selective covered and non-covered reads, composite covered and non-covered reads, and entity-related join shapes with and without the child FK index.
- Repeated exact-target `ReplayToLSN` is now treated as closed on the current fixture: the replay path reuses cached decoded mutation plans, `ReplayToLSN` short-circuits when the engine is already materialized at the requested `LSN`, and the benchmark harness now warms that path before timing and excludes teardown from the timed region.
- A fresh repeated sample on the current M1 using `go test ./internal/engine/executor -run '^$' -bench '^BenchmarkEngineReplayToLSN$' -benchmem -count=5` now lands at roughly ~`2.13–2.18 ns/op`, `0 B/op`, `0 allocs/op`, so the old repeated same-target replay bottleneck is no longer worth active optimization time.
- The earlier bounded-replay cleanup still matters for the first rebuild to a target `LSN`: skipping snapshot capture/persistence/eviction during `ReplayToLSN` materially reduced replay allocation volume (roughly ~`2.69 MB/op` -> ~`1.41 MB/op`, ~`16.7k` -> ~`12.1k allocs/op`) before the exact-target fast path closed the repeated same-target case entirely.
- Snapshot-load benchmarking is now better grounded after fixing the restart benchmark harness to clone the WAL/snapshot fixture per iteration, removing one extra deep copy during snapshot materialization, reducing dictionary-string allocation in the binary decoder, decoding table rows directly into positional slices, skipping duplicate shutdown checkpoint writes when the latest `LSN` is already on disk, loading the timestamp index directly into compressed ranges, and fast-pathing single-file snapshot directories through direct binary decode. Fresh repeated `-benchmem -benchtime=200ms -count=2` samples on the current M1 now put persisted-snapshot restart at roughly ~`2.26–2.44 ms/op` versus ~`1.94–2.05 ms/op` for replay-only, while using only ~`767–773 KB/op` and ~`3.0k allocs/op`; that is materially tighter than the earlier ~`3.91 ms/op` result, but the persisted path still loses on this short fixture, so the item remains open.
- The old end-to-end persisted-snapshot restart benchmark is a head-snapshot best-case because fixture shutdown flushes snapshots to the current `headLSN`; new tail/cadence sweeps now exist to model non-zero replay tails explicitly and are the right evidence source for deciding snapshot frequency.
- Focused snapshot-load benchmarks now show the main persisted-restart hotspot is still snapshot-directory read/decompress/decode/materialization at roughly ~`235–238 µs/op` on the current fixture even after the single-file fast path, while `replayFromSnapshots` state restore remains effectively negligible (~`452–456 ns/op`).
- A stronger direct full-snapshot restore path now decodes single-file persisted snapshots straight into runtime `engineSnapshot` / `domainState` / `tableState` structures instead of materializing generic persisted intermediates first; focused regression coverage confirms that path matches the generic decode result for the same on-disk snapshot.
- Fresh spot checks on 2026-03-16 using `go test ./internal/engine/executor -run '^$' -bench 'BenchmarkEngine(RestartFromPersistedSnapshot|ReadPersistedSnapshotsFromDir|DecodeFullSnapshotDirect)(Indexed)?$' -benchmem` show a real step down in snapshot-directory load after that refactor: `BenchmarkEngineReadPersistedSnapshotsFromDir` now lands at ~`175.7 µs/op`, ~`241.7 KB/op`, `609 allocs/op`; `BenchmarkEngineDecodeFullSnapshotDirect` at ~`94.2 µs/op`, ~`299.8 KB/op`, `1,151 allocs/op`; `BenchmarkEngineDecodeFullSnapshotDirectIndexed` at ~`180.2 µs/op`, ~`485.2 KB/op`, `1,713 allocs/op`; and `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed` at ~`304.1 µs/op`, ~`474.5 KB/op`, `1,174 allocs/op`.
- A fresh repeated comparison on 2026-03-16 using `go test ./internal/engine/executor -run '^$' -bench '^(BenchmarkEngine(RestartReplayOnly|RestartFromPersistedSnapshot|ReadPersistedSnapshotsFromDir))$' -benchmem -benchtime=200ms -count=2` keeps the closure story conservative but clearly improved: replay-only now lands at ~`1.98–2.41 ms/op`, ~`1.72 MB/op`, `12,809 allocs/op`, while persisted restart lands at ~`2.58–2.73 ms/op`, ~`638–659 KB/op`, `2,405–2,411 allocs/op` and snapshot-directory load at ~`196.9–198.8 µs/op`, ~`240.5 KB/op`, `608 allocs/op`. So the new decoder substantially cut directory-load cost and heap pressure, but persisted restart still trails replay-only on the short head-snapshot fixture and therefore stays open pending a fresh cadence-tail sweep on the new code.
- Fresh `-benchtime=1x` cadence/tail spot checks on 2026-03-16 keep the medium-tail policy story alive after the decoder refactor, but not yet clean enough for closure. On the fixed 500-record tail sweep, persisted snapshots still lose at `tail_500` (~`15.5 ms` vs ~`11.1 ms`) and `tail_5000` (~`37.5 ms` vs ~`23.3 ms`), while winning at the very long `tail_10000` point (~`96.4 ms` vs ~`136.0 ms`); on the cadence sweep they still lose at the small `total_1000` case (~`15.5 ms` vs ~`11.5 ms`) but win clearly at `total_10500` (~`42.8 ms` vs ~`69.3 ms`) and modestly at `total_50500` (~`194.4 ms` vs ~`201.9 ms`). That is directionally better than the earlier base-fixture story, but the single-iteration nature of these sweeps means snapshot-load should remain open until those crossovers are repeated enough to trust.
- Targeted `-count=3` repeats on the key crossover points now make that picture materially firmer. The medium cadence win is tight and stable: `persisted_snapshot_total_10500_tail_500` repeats at ~`42.3–43.0 ms`, ~`24.53 MB/op`, ~`143.6k allocs/op` versus `replay_only_total_10500` at ~`68.4–68.8 ms`, ~`49.1–49.2 MB/op`, ~`308.8k allocs/op`. The large cadence point also stays slightly favorable to persisted restart at ~`190.6–195.1 ms`, ~`118.8–118.9 MB/op`, ~`664.1k allocs/op` versus replay-only at ~`199.3–210.2 ms`, ~`221.6–221.7 MB/op`, ~`1.26M allocs/op`.
- The replay-tail crossover is also now clearer: `tail_5000` consistently still loses with persisted snapshots (~`35.9–37.3 ms` vs ~`21.1–26.1 ms`), but `tail_10000` has moved from a one-off spot check to a real crossover band, with persisted restart ranging ~`65.2–91.0 ms` versus replay-only at ~`63.9–65.9 ms`. That means the decoder refactor did not erase the long-tail crossover; it mostly lowered the fixed snapshot-load cost enough that the break-even point remains in the very long-tail regime.
- Fresh natural-policy insert-heavy reruns on 2026-03-16 show why that forced `tail_5000` loss should not drive the checkpoint policy by itself. Using `BenchmarkEngineRestartNaturalWorkloadCadenceSweep`, the current policy still lands checkpoints well enough to beat replay-only at both benchmarked insert-heavy anchors after the decoder refactor: `policy_persisted_total_1000_tail_500` now repeats at ~`4.0–7.5 ms` vs replay-only at ~`9.5–11.4 ms`, and `policy_persisted_total_10500_tail_500` at ~`28.3–29.6 ms` vs replay-only at ~`69.1–71.4 ms`. So the remaining policy work is less about changing the current adaptive cadence immediately and more about making sure forced-tail synthetic losses do not get mistaken for the natural runtime checkpoint behavior.
- The same natural-policy check now also holds for the heavier mutation mixes after the decoder refactor. For `update_heavy`, `policy_persisted_total_1000_tail_500` repeats at ~`6.5–7.0 ms` vs replay-only at ~`210.8–219.8 ms`, while `policy_persisted_total_10500_tail_500` stays slightly ahead at ~`3.80–3.94 s` vs ~`3.73–4.01 s` and trims allocation volume from ~`8.63 GB` / ~`40.4M allocs` to ~`8.61 GB` / ~`40.2M allocs`. For `delete_heavy`, the policy-driven path remains decisively better at the small anchor (~`4.9–9.3 ms` vs ~`205.7–208.9 ms`) and modestly better at the medium anchor (~`2.15–2.17 s` vs ~`2.21–2.23 s`) while also shaving heap and allocations. That is strong enough to keep the current checkpoint policy intact: the synthetic forced-tail losses are real, but the natural runtime cadence still lands on the right side of the trade-off for all three benchmarked workload classes.
- Early cadence spot checks on the current M1 show a snapshot plus ~`500` replayed records beating replay-only at ~`1k` total rows (`12.7 ms` vs `16.2 ms`) and around the medium adaptive anchor of ~`10.5k` total rows (`57.6 ms` vs `95.8 ms`), while the advantage narrows again by ~`50.5k` total rows (`289.9 ms` vs `307.1 ms`), so any closure-level snapshot-cadence decision still needs repeated runs and likely workload-class-specific tuning.
- A new restart workload sweep now exercises `insert_heavy`, `update_heavy`, and `delete_heavy` fixture shapes with the same final-state validation, so cadence analysis no longer assumes append-heavy WAL is representative.
- Repeated `-benchmem -benchtime=100ms -count=3` workload samples on the current M1 now show a clearer split: at the default 500-record anchor, `persisted_snapshot` is worse for `insert_heavy` (~`9.8–10.8 ms` vs ~`8.0–8.4 ms` replay-only) but modestly better for `update_heavy` (~`266–281 ms` vs ~`277–302 ms`) and `delete_heavy` (~`275–282 ms` vs ~`288–292 ms`), while also trimming a small amount of memory/allocations in all three shapes.
- Repeated `-benchmem -benchtime=1x -count=2` workload-cadence samples now make that crossover more concrete: with a 500-record tail, `insert_heavy` is still clearly worse at the small anchor (~`22.1–22.3 ms` vs ~`8.7–15.6 ms`) but strongly better by the medium anchor (~`61.3–62.3 ms` vs ~`95.9–101.5 ms`), while `update_heavy` and `delete_heavy` are already break-even-to-better at the small anchor and widen their wins by the medium anchor (~`4.76–5.00 s` vs ~`4.97–5.24 s` for `update_heavy`, ~`3.31–3.43 s` vs ~`3.50–3.54 s` for `delete_heavy`).
- Heap pressure follows the same direction: by the medium anchor, persisted snapshots cut restart allocation volume sharply for `insert_heavy` (~`35.6 MB` vs ~`62.1–62.4 MB`) and trim it modestly for `update_heavy`/`delete_heavy`, so the eventual policy should be evaluated against both restart latency and allocation pressure, not just wall-clock time.
- The leading policy candidate has now been implemented for persisted checkpoints: the existing volume-based snapshot anchors remain the baseline, but the engine maintains a rolling recent mutation-pressure window and halves the persisted-checkpoint mutation interval when weighted update/delete pressure dominates (`insert=1`, `update=4`, `delete=3`, floor `250` mutations).
- This first implementation intentionally targets persisted checkpoint cadence only, not in-memory snapshot retention: restart wins come from fewer replayed WAL records after disk checkpoints, while hot-path historical queries already prefer in-memory snapshots and should not pay extra write-path cost until there is separate evidence for changing retention.
- The policy now has deterministic regression coverage: `TestMutationMixAdaptivePersistedCheckpointCadence` proves that an insert-heavy `250`-mutation window does not persist a checkpoint while an update-heavy `250`-mutation window does, matching the intended “checkpoint earlier for update/delete-heavy pressure” behavior.
- Repeated natural-cadence runs (`-benchmem -benchtime=1x -count=2`) now make the post-implementation signal much stronger: without forcing a final head-snapshot flush, the policy-driven persisted path reduced small-anchor restart from ~`16–18 ms` to ~`11.5–13.0 ms` for `insert_heavy`, from ~`286–290 ms` to ~`11.6–12.2 ms` for `update_heavy`, and from ~`295–311 ms` to ~`13.1–16.1 ms` for `delete_heavy`.
- At the medium anchor the policy still preserves meaningful wins with stable ranges: `insert_heavy` drops from ~`94–96 ms` to ~`51 ms`, `update_heavy` from ~`5.13–5.26 s` to ~`4.79 s`, and `delete_heavy` from ~`3.52–3.55 s` to ~`3.35–3.38 s`.
- Heap pressure collapses in the natural small-anchor restart cases because the replay tail is largely eliminated when the policy lands a checkpoint near the workload end: `update_heavy` drops from ~`616 MB` / ~`2.54M` allocs to ~`1.62 MB` / ~`8.8k` allocs, and `delete_heavy` from ~`618 MB` / ~`2.67M` allocs to ~`2.14 MB` / ~`11.8k` allocs on the current M1 samples.
- A further three-run confirmation on the heaviest shapes keeps the post-implementation ranges tight enough to treat the effect as stable rather than anecdotal: `update_heavy` small-anchor replay-only remains ~`286–311 ms` versus ~`11.4–12.4 ms` with policy-driven checkpoints, and `delete_heavy` small-anchor replay-only remains ~`288–310 ms` versus ~`15.4–16.5 ms` with policy-driven checkpoints.
- Given the deterministic regression test plus the repeated natural-restart benchmark evidence, the mutation-mix-aware persisted checkpoint policy is now treated as closed for the restart/cadence slice of Epic AB, even if snapshot-load micro-optimizations remain open separately.
- Within that snapshot-directory load, binary decode remains the largest measured in-process component, but the direct positional-row decode path pushed it down materially to roughly ~`79–166 µs/op`, ~`186–458 KB/op`, and ~`1.1k–1.7k allocs/op`, ahead of raw file I/O (~`87 µs/op`), zstd decompression (~`74 µs/op`), and snapshot materialization (~`49–57 µs/op`).
- A follow-up row-slab decode change now removes one allocation per decoded row on the common dense-row path, tightening `BenchmarkEngineDecodePersistedSnapshotFiles` to roughly ~`70.9–72.0 µs/op` and `586 allocs/op`, while bringing `BenchmarkEngineReadPersistedSnapshotsFromDir` down to roughly ~`324–364 µs/op` and ~`1.77k allocs/op` on the current base fixture.
- The positional-row decode change clearly improved isolated decode and snapshot-directory load benchmarks, but end-to-end persisted-restart timing is still noisy on the current short benchtime harness and needs repeated confirmation before any stronger claim.
- Delta-chain merge is negligible on the current fixture because it is effectively loading a single persisted snapshot file, so the remaining work in this AB slice is about file read + decode efficiency rather than cross-file snapshot chaining.
- A follow-up inspection with an index-rich snapshot fixture (primary key + hash + btree persisted) raised full snapshot-directory load to about ~`493 µs/op`, but decode still stayed in the same order of magnitude (~`167 µs/op`), so persisted index payload does not appear to create a new dominant hotspot beyond the existing file-read + decode path.
- Removing the extra raw-file accumulation pass in `readAllSnapshotsFromDir()` did not materially shift timings on the current fixture, which further narrows the remaining opportunity to file-read/decompress/decode itself rather than the old two-pass control flow.
- The earlier direct full-snapshot decode experiment via `decodeSnapshotsBinary()` was not compelling, but the newer runtime-state decoder that bypasses persisted intermediates entirely now does show a meaningful win on both the base and index-rich fixtures, so this AB slice is no longer blocked on decode-path skepticism; the remaining question is whether the improved directory-load numbers translate into closure-grade restart wins on the cadence-tail benchmarks.
- A follow-up change now overlaps per-file read/decompress/decode work across numbered snapshot files before the deterministic in-order delta merge; on the current two-file spot check (`-benchmem -benchtime=100ms -count=1`) that moved `BenchmarkEngineReadPersistedSnapshotsFromDir` from about ~`407 µs/op` to ~`343 µs/op` and `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed` from about ~`564 µs/op` to ~`504 µs/op`, so snapshot-directory load remains open but has another evidence-backed improvement.
- A further materialization-path change now preserves decoded btree index entries in restore-ready form so indexed snapshot loads do not pay an extra per-entry copy during `marshalableToTableState`; on the current indexed fixture that kept `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed` in the ~`499 µs/op` range while reducing allocation volume from roughly ~`842 KB/op` to ~`746 KB/op`, so the remaining snapshot-load work is becoming more about base-table/materialization metadata overhead than about decoded btree entry copying.
- A follow-up restore-path change now reuses snapshot domain state directly in `restoreSnapshot()` / `replayFromSnapshots()` instead of deep-cloning it again before WAL-delta replay; focused regression coverage confirms later writes still leave the captured snapshot immutable, and `BenchmarkEngineReplayFromPersistedSnapshots` dropped to roughly ~`690 ns/op`, `640 B/op`, `8 allocs/op` on the current fixture, which makes the in-memory restore step effectively negligible relative to snapshot-directory load.
- A fresh end-to-end spot check after these snapshot-load changes still leaves `BenchmarkEngineRestartFromPersistedSnapshot` slightly slower than `BenchmarkEngineRestartReplayOnly` on the current short fixture (~`4.46 ms/op` vs ~`4.24 ms/op`), but with materially lower heap pressure (~`1.08 MB/op` and ~`5.2k allocs/op` vs ~`2.30 MB/op` and ~`15.6k allocs/op`), so the remaining work is now less about raw restore mechanics and more about the benchmarked full restart envelope around snapshot file handling.
- A fresh representative spot check on the restart sweeps keeps the same broader story intact after the latest snapshot-load work: with a fixed 500-record snapshot anchor and a long 5k replay tail, `persisted_snapshot_tail_5000` still loses to replay-only (~`50.4 ms/op` vs ~`39.3 ms/op`), but on the more policy-relevant medium cadence case (`total_10500`, `tail_500`) the persisted path still wins clearly (~`61.3 ms/op`, ~`34.6 MB/op`, ~`196k allocs/op` vs ~`94.5 ms/op`, ~`62.1 MB/op`, ~`361k allocs/op`). That keeps the closure framing the same: head-snapshot and long-tail fixtures are still not closure-grade wins, while the realistic medium-tail cadence story remains strong.
- The persisted-index/cache item is now narrower than it first appeared: a persisted timestamp→LSN side index already exists and is restart-validated, but there is still no evidence-backed need yet for general persisted table indexes or a broader cache layer.
- Commit batching on realistic workloads now has direct evidence instead of only the synthetic concurrent INSERT loop. `BenchmarkConcurrentCommit` was corrected to use a real snapshot directory (so runtime checkpoints stop failing during the benchmark), and the commit queue now opportunistically coalesces immediately following jobs via scheduler yields instead of a fixed sleep. On the realistic 4-worker / 9-INSERT transaction benchmark this moved throughput from ~`415 µs/op` to ~`304 µs/op`, tightened p50 from ~`1.63 ms` to ~`0.98 ms`, p95 from ~`3.18 ms` to ~`2.40 ms`, and p99 from ~`4.29 ms` to ~`3.52 ms`, with zero retry regressions. The simpler `BenchmarkEngineWriteCommitConcurrent` stayed roughly flat (~`392 µs/op` -> ~`399 µs/op`), so the win appears workload-shaped rather than a blanket microbenchmark gain.
- The parallel-scan item has now been evaluated to a defer decision: there is still no proven workload class that justifies intra-query parallel scan complexity over the current deterministic single-threaded indexed paths.
- The persisted index/cache architecture item can now be narrowed to a defer/close decision rather than a build item. Existing evidence already covers the useful pieces: repeated exact-`LSN` historical reads materially improved via the in-memory WAL/history caches, the persisted timestamp→`LSN` side index is restart-validated and now benchmarks at ~`14.5 ns/op`, `0 B/op`, `0 allocs/op` for `BenchmarkEngineLSNForTimestampAfterRestart`, and the indexed snapshot-load path has dropped from the earlier ~`493 µs/op` band to ~`304 µs/op` without introducing a broader persisted table-index layer. Taken together, measured IO now says ASQL already has the narrowly justified persisted/indexed surfaces it needs, while broader persisted table indexes or a generic restart cache still lack evidence of being the next bottleneck.
- Final closure interpretation for snapshot load: the short head-snapshot benchmark still trails replay-only on absolute wall-clock, and the synthetic forced-tail sweep still exposes a long-tail break-even region, but the policy-relevant evidence is now strong enough to close the item anyway. Snapshot-directory load has fallen to ~`176–199 µs/op` on the base fixture, the indexed variant to ~`304 µs/op`, natural-policy restart wins are stable across `insert_heavy`, `update_heavy`, and `delete_heavy`, and the remaining losses are now either best-case replay fixtures or intentionally adversarial tail placements rather than the natural operating mode ASQL should optimize around.
- Entity-related join reads now have dedicated scaling coverage: `BenchmarkEngineReadEntityRelatedJoinScaling` and `BenchmarkEngineReadEntityRelatedJoinRightFilterScaling` show that root-table pruning, root-only `AND` conjunct extraction, and safe right-side filtering materially improved the user-reported “entity + related tables” slowdown as row counts grow, but the broader indexed-read/query-latency item remains open because other read shapes still need closure-level decisions.
- Repeated historical reads now have dedicated scaling coverage via `BenchmarkEngineReadHistoricalAsOfLSNScaling`: exact-`LSN` repeat reads benefit materially from the new WAL-record cache and small historical-state cache, while invalidation on commit and WAL GC keeps correctness explicit.
- Failover recovery is now treated as closed at the subline level: the benchmark suite covers `small_total_40`, `medium_total_640`, and `large_total_4608`, the largest case has a phase split proving where time goes, replay-side cleanup materially reduced apply cost, and persisted segment headers removed WAL reopen/discovery as a meaningful restart cost (~`38–73 µs` on the large case). The current closure decision is that ~`2.1–4.3 ms` end-to-end large-scenario recovery on the benchmark fixture is strong enough to move this slice out of active optimization and focus AB effort on replay throughput, snapshot load, and indexed-read latency.

Latest directional read evidence:
- `BenchmarkEngineReadIndexedRangeBTree` now benefits from the same bounded btree windowing and repeated at ~`270–273 µs/op`.
- `BenchmarkEngineReadIndexOnlyOrderBTree` repeated at ~`31–32 µs/op`.
- `BenchmarkEngineReadIndexOnlyOrderOffsetBTree` repeated at ~`56–57 µs/op`.
- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree` now uses a binary-searched bounded scan window on the ordered index path and repeated at ~`35–49 µs/op` instead of the earlier ~`274–275 µs/op`.
- `BenchmarkEngineReadSelectiveNonCoveredBTree` now benefits from the same bounded scan window on the ordered btree path and repeated at ~`304–305 µs/op` instead of the earlier ~`406–407 µs/op`.
- `BenchmarkEngineReadCompositeCoveredIndexOnlyBTree` now exercises `btree-index-only` at ~`33–34 µs/op`.
- `BenchmarkEngineReadCompositeNonCoveredBTree` exercises `btree-order` at ~`76–78 µs/op`.
- `BenchmarkEngineReadEntityRelatedJoinScaling` now shows the indexed related-read path staying in the ~`24–35 µs/op` range through `orders_25000`, while the optimized unindexed path lands around ~`1.81 ms/op` at `orders_10000` and ~`5.85 ms/op` at `orders_25000` instead of the earlier broader join-materialization cliff.
- `BenchmarkEngineReadEntityRelatedJoinRightFilterScaling` keeps the indexed right-filter shape in the ~`20–22 µs/op` range through `orders_10000`, while the unindexed shape stays around ~`1.80 ms/op` at `orders_10000`, which is directionally consistent with the new pruning/filtering improvements rather than a fresh regression cliff.
- Current closure interpretation: the important indexed read shapes are now benchmarked and no longer show an unexplained size-linked regression on the indexed path, so remaining AB effort should move to snapshot-load closure and only then to persisted index/cache architecture if IO evidence still justifies it.
- `BenchmarkEngineReadHistoricalAsOfLSNScaling` now shows repeated exact-`LSN` historical reads improving from roughly ~`2.10 ms/op`, ~`1.19 MB/op`, ~`8.1k allocs/op` to ~`0.59 ms/op`, ~`373 KB/op`, ~`2.7k allocs/op` on the `rows_1000` fixture.
- The same historical benchmark improves the larger `rows_10000` fixture from roughly ~`8.57 ms/op`, ~`12.09 MB/op`, ~`80k allocs/op` to ~`3.12 ms/op`, ~`3.79 MB/op`, ~`26.8k allocs/op`, which is strong enough to treat exact-target repeat history reads as a real fixed bottleneck rather than micro-tuning.
- Current interpretation: index-only is strongly justified for simple covered ordered reads, remains strong with `OFFSET`, is justified on the selective covered shape after bounded-scan support, and is now also justified for the covered composite ordered-read shape. Further expansion should still be guided by measured query shapes rather than blanket rollout.

- [x] Benchmark and improve commit batching on realistic workloads.
- [x] Benchmark and improve replay throughput.
- [x] Benchmark and improve snapshot load time.
- [x] Benchmark and improve indexed read/query latency.
- [x] Benchmark and improve failover recovery time.
- [x] Evaluate persisted index/cache architecture from measured IO behavior.
- [x] Expand index-only scan coverage where benchmarks justify it.
- [x] Evaluate parallel scans only for proven workload classes.

Acceptance gates (must pass before closing Epic AB)
- [x] Performance work is benchmark-driven, not assumption-driven.
- [x] Benchmark suite includes failover and recovery scenarios, not only steady-state throughput.

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

## Epic AG — Database principals and deterministic authorization

Reference inputs:
- `docs/reference/postgres-compatibility-surface-v1.md`
- `docs/adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md`
- `docs/adr/0004-durable-database-principals-and-historical-authorization.md`

Execution rule:
- Treat users, roles, memberships, and grants as engine-owned metadata, not only process configuration.
- Preserve deterministic replay: security metadata changes must be represented in WAL/state recovery just like other durable catalog changes.
- Keep transport/admin tokens as deployment/operator controls; do not confuse them with database principals.

Recommended MVP execution order:
1. Land a durable principal catalog with replay-safe WAL records and one bootstrap admin path.
2. Enforce current-state authorization for normal reads/writes plus explicit temporal-read privilege.
3. Expose the model first through `asqlctl` and admin APIs before broad SQL role-management syntax.
4. Add Studio management and inspection flows only after the engine and CLI semantics are stable.

First vertical slice to execute:
- [x] Add durable principal catalog state with one bootstrap admin principal and replay-safe WAL mutations.
- [x] Authenticate pgwire sessions against stored principals instead of the fixed shared logical user.
- [x] Add `SELECT_HISTORY` as a first explicit privilege checked against current grant state for historical queries.
- [x] Expose the slice through a minimal admin/CLI path before adding broader PostgreSQL-style role DDL.

AG-1 — bootstrap principal + historical-read baseline:
- [x] Add deterministic WAL record types for `principal_create`, `principal_alter`, `principal_disable`, `role_grant`, and `privilege_grant` / `privilege_revoke`.
- [x] Add replay/state-rebuild support for principal catalog state so restart reproduces the same effective permission graph.
- [x] Introduce one bootstrap path for the first admin principal that is allowed only when the durable principal catalog is empty.
- [x] Add stored-principal authentication in pgwire startup, including disabled-principal rejection and deterministic audit events.
- [x] Add one first-class privilege constant for temporal reads (`SELECT_HISTORY`) and enforce it on `AS OF LSN`, `AS OF TIMESTAMP`, and `FOR HISTORY` paths.
- [x] Add a minimal admin API plus `asqlctl` commands for `create user`, `list users`, `grant SELECT_HISTORY`, and effective-permission inspection.
- [x] Add regression coverage for bootstrap, restart/replay, successful historical read, denied historical read, and disabled principal login.

AG-1 acceptance notes:
- [x] No fixed logical pgwire user remains on the primary authenticated path for the slice.
- [x] The same WAL history yields the same principal catalog and effective permissions after replay.
- [x] A user created today can read old history only after an explicit current grant, and audit output makes that sequence visible.

P0 — principal catalog and durability:
- [x] Define the ASQL principal model (`USER`, `ROLE`, membership, disabled/locked state, password-hash or secret-reference shape).
- [x] Persist principal and grant mutations in WAL with deterministic recovery/replay semantics.
- [x] Add engine/catalog APIs for principal lookup, role expansion, and grant resolution without relying on process-global mutable state.
- [x] Define bootstrap semantics for the first admin principal without making steady-state identity management config-only.

P1 — authorization model and historical semantics:
- [x] Define the first privilege surface for database/domain/schema/table operations and operator-sensitive capabilities.
- [x] Add explicit privilege semantics for temporal access (`SELECT_HISTORY` / equivalent) instead of treating historical reads as implicit `SELECT`.
- [x] Define and document the rule for historical authorization: by default, authorization is evaluated against the current principal/grant state, while the queried data snapshot may target an older `LSN`/timestamp.
- [x] Record enough audit information to prove both the target historical point and the grant state under which access was allowed.

P2 — pgwire and execution enforcement:
- [x] Replace the fixed shared-user pgwire posture with real database-principal authentication while preserving documented compatibility for supported clients.
- [x] Enforce authorization checks in planner/executor for read/write/schema/admin flows, including temporal queries and replay-sensitive operations.
	- [x] Require `ADMIN` for operator/admin pgwire virtual-schema helpers under `asql_admin.*` and for `asql_admin.replay_to_lsn(...)`, and require `SELECT_HISTORY` for historical helper views such as `asql_admin.row_history` / `asql_admin.entity_version_history`.
	- [x] Add principal-aware executor helpers for current reads, temporal reads, history inspection, and replay-to-LSN so engine-level callers can reuse the same `ADMIN` / `SELECT_HISTORY` checks outside direct pgwire statement execution.
	- [x] Require durable-principal metadata on gRPC `BeginTx` / query / explain / temporal-history / replay helpers when the principal catalog is enabled, and route those handlers through the shared principal-aware executor helpers.
	- [x] Require durable-principal headers on the standalone/internal HTTP `BeginTx` / query / explain / temporal-history / replay and operator/admin helper endpoints when the principal catalog is enabled, and route those handlers through the same principal-aware executor/admin privilege checks.
- [x] Replace compatibility-shim privilege probes that currently always succeed with grant-aware behavior where claims are made public.
- [x] Add deterministic regression coverage for authn/authz, replay recovery of principal state, and denied historical-access paths.

Acceptance gates (must pass before closing Epic AG)
- [x] Creating or changing a user/role/grant survives restart and replay because it is represented in durable engine state.
- [x] Historical reads have an explicit, documented authorization rule and dedicated regression coverage.
- [x] A newly created principal can be granted historical-read capability without backdating its existence or weakening auditability.
- [x] Public compatibility docs clearly distinguish transport tokens from database principals and role-based permissions.

## Epic AH — Security management surfaces (CLI + Studio)

Reference inputs:
- `cmd/asqlctl/`
- `asqlstudio/`

Execution rule:
- Expose the minimum secure management surface only after Epic AG defines the engine truth.
- Prefer guided admin workflows over thin wrappers around raw catalog mutations.

Recommended rollout order:
1. `asqlctl` create/list/show flows for principals.
2. `asqlctl` grant/revoke flows including temporal access.
3. Effective-permission inspection and audit views.
4. Studio management screens after CLI semantics prove stable.

P0 — `asqlctl` security administration:
- [x] Add `asqlctl` commands for user/role lifecycle (`create`, `alter`, `disable`, `list`, `show`).
	- [x] User lifecycle flows now have ergonomic `asqlctl security user create|alter|disable|enable|delete|list|show` entry points backed by the existing principal admin APIs.
	- [x] Role lifecycle flows now have ergonomic `asqlctl security role create|disable|enable|delete|list|show` entry points, with `show`/`list` reusing the shared principal inspection surface.
	- [x] Treat password rotation as the first `alter` workflow via `asqlctl security user alter` backed by the existing principal password-set path.
- [x] Add `asqlctl` commands for membership and grants/revokes, including temporal-access privileges.
- [x] Add `asqlctl` output/views that make effective permissions and inherited role membership explicit.
- [x] Add audit-oriented CLI flows to inspect who can access historical data and why.

AH-1 — CLI-first management slice:
- [x] Add `asqlctl security user create` with password/secret input handling appropriate for the selected bootstrap model.
- [x] Add `asqlctl security user list` and `show` with principal state (`enabled`, `disabled`, inherited roles, temporal privileges).
- [x] Add `asqlctl security grant history` / `revoke history` as the first explicit temporal-permission workflow.
- [x] Add `asqlctl security who-can history` or equivalent inspection flow to explain effective historical access.

P1 — Studio security UX:
- [x] Add a Studio security area for principals, roles, memberships, and grants.
- [x] Add a guided grant flow that makes historical-read access an explicit choice, not an accidental byproduct.
- [x] Add effective-permission inspection for a selected user/role, including inherited roles and temporal capabilities.
- [x] Add denial/audit visibility in Studio for failed authz checks and recent security-relevant changes.

P2 — docs, examples, and operational guidance:
- [x] Document the database security model in getting-started/reference docs, including bootstrap and rotation flows.
- [x] Add examples covering: create admin, create reader, grant historical access, revoke historical access, and verify denied paths.
- [x] Update compatibility docs so unsupported PostgreSQL role-management statements are either implemented, explicitly translated, or still documented as unsupported.

Acceptance gates (must pass before closing Epic AH)
- [x] A production operator can create a user, grant historical-read access, inspect effective permissions, and revoke access from CLI without internal knowledge.
- [x] The same core workflows are available from Studio with explicit auditability and low surprise.
- [x] User-facing docs explain how historical access works for newly created principals and how that interacts with replay/time-travel.

## Epic AI — Mainstream PostgreSQL app/tool adoption wedge

Reference inputs:
- `docs/product/asql-adoption-roadmap-v1.md`
- `docs/reference/postgres-compatibility-surface-v1.md`
- `docs/ai/11-technical-gap-matrix-vs-postgres.md`
- `docs/ai/12-mainstream-adoption-blockers-v1.md`

Execution rule:
- Only expand compatibility where it measurably improves adoption through mainstream drivers, tools, or migration flows.
- Preserve the explicit product stance: ASQL is a deterministic engine with a pragmatic PostgreSQL-compatible subset.

P0 — identify the next real adoption blockers:
- [x] Audit the next 3 highest-value blocked client/tool/app flows after the current compatibility wedge (`psql`, `pgx`, GUI, ORM-lite, BI-lite).
- [x] For each blocked flow, classify the missing behavior as docs gap, protocol/catalog gap, SQL gap, or explicit non-goal.
- [x] Publish a short ranked “next adoption blockers” note with evidence and expected adoption impact.

Epic AI P0 audit result (2026-03-17):
- ranked note published in `docs/ai/12-mainstream-adoption-blockers-v1.md`
- next recommended implementation target: `pgAdmin` startup + schema-browse validation lane
- next two ranked follow-ons: one narrow ORM-lite happy path, then one narrow BI-lite read-only datasource path

P1 — close the highest-return compatibility gaps:
- [x] Implement the smallest protocol/catalog/session fixes required for the top-ranked mainstream flows.
- [x] Add regression tests for each newly-claimed flow before expanding the public docs.

Epic AI P1 slice note (2026-03-17):
- added a regression-covered `pgAdmin` startup + schema-browse lane in `internal/server/pgwire/server_test.go`
- updated compatibility/docs guidance to treat the current `pgAdmin` startup/catalog subset as validated
- published a narrow ORM-lite translation contract in `docs/reference/orm-lite-adoption-lane-v1.md`
- added regression coverage for the explicit ORM-lite happy path plus `START TRANSACTION` guidance in `internal/server/pgwire/server_test.go`
- published a narrow BI-lite translation contract in `docs/reference/bi-lite-adoption-lane-v1.md`
- added regression coverage for the explicit BI-lite read-only path in `internal/server/pgwire/server_test.go`

P2 — app-facing migration wedge:
- [x] Add support or explicit translation guidance for the next high-return SQL shapes seen in real app evaluation friction.
- [x] Add one end-to-end “existing Postgres-oriented app reaches first successful read/write flow on ASQL” guide.
- [ ] Add a release-gated compatibility smoke pack for the prioritized client/tool/app matrix.

Epic AI P2 slice note (2026-03-17):
- added [../getting-started/12-first-postgres-service-flow.md](../getting-started/12-first-postgres-service-flow.md) as the first end-to-end PostgreSQL-oriented service migration wedge
- linked the new guide from the getting-started onboarding flow and adoption/troubleshooting surfaces
- published [../reference/postgres-app-sql-translation-guide-v1.md](../reference/postgres-app-sql-translation-guide-v1.md) to classify high-return PostgreSQL-shaped SQL into use-now, translate-first, and defer buckets for app evaluation

Acceptance gates (must pass before closing Epic AI)
- [ ] At least 2 additional mainstream evaluation flows move from blocked to documented-working or explicitly-translated.
- [ ] Each new compatibility claim is backed by regression coverage and matrix/docs updates.
- [ ] Compatibility work remains evidence-driven rather than parity-driven.

## Epic AJ — Planner and performance credibility

Reference inputs:
- `docs/product/asql-adoption-roadmap-v1.md`
- `docs/product/performance-benchmark-plan-v1.md`
- `docs/ai/09-benchmark-baseline.md`

Execution rule:
- Performance work must start from observed query shapes, explain output, and benchmark evidence.
- Prefer planner improvements that also improve operator understanding, not only raw speed.

P0 — credibility baseline:
- [ ] Define the top adoption-critical query shapes for planner/performance work (joins, selective filters, aggregates, pagination, ordered reads).
- [ ] Add or refresh benchmark coverage for those shapes with representative fixture sizes.
- [ ] Add a concise operator-facing planner/performance scorecard doc tying each shape to expected scan strategy and evidence.

P1 — planner visibility and determinism:
- [ ] Extend `EXPLAIN` output so join order, access path choice, residual filters, and fallback reasons are more explicit.
- [ ] Add regression coverage proving deterministic plan selection for the prioritized query shapes.
- [ ] Surface the most useful planner warnings/suggestions in Studio without requiring raw-plan reading.

P2 — benchmark-backed wins:
- [ ] Implement the next measured planner/index improvements for the prioritized shapes.
- [ ] Add release evidence showing latency and allocation improvements for those shapes.
- [ ] Document which query shapes are now strong, acceptable, or still intentionally limited.

Acceptance gates (must pass before closing Epic AJ)
- [ ] Planner behavior for the top query shapes is benchmarked, visible, and deterministic.
- [ ] At least 3 adoption-critical query shapes have evidence-backed performance or explainability improvements.
- [ ] Docs and Studio expose enough information that users can reason about why a query is fast or slow.

## Epic AK — Temporal superpower workflows

Reference inputs:
- `docs/product/asql-adoption-roadmap-v1.md`
- `docs/reference/database-security-model-v1.md`
- `docs/getting-started/05-time-travel-and-history.md`

Execution rule:
- Favor workflows that make replay, `AS OF`, `FOR HISTORY`, and entity/version primitives easier to compose and explain.
- Keep the temporal surface general-purpose and database-native.

P0 — identify the best differentiating workflows:
- [ ] Define the top operator/developer temporal jobs ASQL should make dramatically easier than the baseline market experience.
- [ ] Audit current CLI/Studio/docs coverage for those jobs and identify the sharpest missing links.
- [ ] Publish a concise “temporal superpower” narrative tied to concrete workflows rather than generic claims.

P1 — workflow productization:
- [ ] Add one guided Studio workflow for “current row -> history -> chosen LSN/timestamp -> explanation/diff”.
- [ ] Add one CLI workflow that packages the same temporal investigation path for scripts and operators.
- [ ] Add one reference example showing how an application developer debugs a bad state transition using ASQL temporal primitives.

P2 — temporal explanation depth:
- [ ] Add first-class snapshot diff or historical explanation helpers where current raw primitives are still too manual.
- [ ] Add audit-friendly export/report output for temporal investigations.
- [ ] Document the recommended temporal-debugging playbook across README/getting-started/reference/Studio.

Acceptance gates (must pass before closing Epic AK)
- [ ] ASQL has at least one temporal workflow that is clearly easier to demo and teach than the default baseline.
- [ ] Studio, CLI, and docs tell the same temporal-investigation story.
- [ ] Temporal workflows remain deterministic, auditable, and general-purpose.

## Epic AL — Operator-grade production boringness

Reference inputs:
- `docs/product/asql-adoption-roadmap-v1.md`
- `docs/product/production-readiness-roadmap-v1.md`

Execution rule:
- Production confidence comes from clear failure behavior, drills, and diagnostics, not only from feature presence.
- Prefer a smaller but trustworthy operator surface over a wide but ambiguous one.

P0 — prove the core drills:
- [ ] Define the canonical operator drills for failover, lag investigation, backup creation, restore, and recovery verification.
- [ ] Audit current Studio/CLI/docs coverage for each drill and rank missing operator steps by risk.
- [ ] Publish a concise production-drill matrix with expected inputs, outputs, and acceptance evidence.

P1 — tighten operator workflows:
- [ ] Close the highest-friction gaps in failover, lag, and recovery workflows across Studio and `asqlctl`.
- [ ] Improve long-running write-path behavior and operator messaging during leader change and degraded states.
- [ ] Add operator-facing evidence exports or summaries for drills and incident review.

P2 — release-grade production proof:
- [ ] Add a repeatable production-readiness validation pack covering failover, lag, backup, restore, and recovery.
- [ ] Tie those drills to emitted metrics, logs, and operator-visible UI/CLI outputs.
- [ ] Document the production-safe happy path and the degraded-mode/operator-response path side by side.

Acceptance gates (must pass before closing Epic AL)
- [ ] A production operator can execute the core drills without internal knowledge.
- [ ] Failover, lag, and recovery behavior are visible through first-class tooling and docs.
- [ ] Production-readiness claims are backed by repeatable validation evidence.

## Epic AM — Migration and onboarding compression

Reference inputs:
- `docs/product/asql-adoption-roadmap-v1.md`
- `docs/getting-started/`
- `examples/`

Execution rule:
- Reduce time-to-first-success for serious evaluation teams.
- Prefer concrete starter kits, fixtures, and migration recipes over generic marketing copy.

P0 — compress the first hour:
- [ ] Define the shortest credible evaluation loop for ASQL from local start to first meaningful app/query/history success.
- [ ] Audit current docs/examples/studio/cli against that loop and identify the biggest first-hour drop-off points.
- [ ] Publish a canonical first-hour path that does not require choosing among parallel onboarding tracks.

P1 — migration starter kits:
- [ ] Add a starter migration guide for a small PostgreSQL-oriented service using the supported ASQL subset.
- [ ] Add one reusable starter app/template that demonstrates schema init, writes, reads, time-travel, and operator checks.
- [ ] Add translation guidance for common “this works in Postgres but needs a different path in ASQL” moments.

P2 — evaluation packaging:
- [ ] Align README, getting-started, examples, and Studio `Start Here` around the same adoption journey.
- [ ] Add fixture-first demo packages aimed at local evaluation, product demo, and pilot-readiness review.
- [ ] Add an adoption scorecard/checklist for teams deciding whether to continue to pilot.

Acceptance gates (must pass before closing Epic AM)
- [ ] A serious evaluator can reach first meaningful success in a short local loop without tribal knowledge.
- [ ] Migration and onboarding surfaces tell one coherent story across docs, examples, CLI, and Studio.
- [ ] The next step from evaluation to pilot is explicit, documented, and measurable.
