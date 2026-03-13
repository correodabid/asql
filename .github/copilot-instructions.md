# ASQL – Copilot Instructions

## Project intent
Build and maintain **ASQL**, a general-purpose deterministic SQL engine in Go.

Core product characteristics:

- embedded-first and single-node usable,
- explicit domain isolation,
- append-only WAL as source of truth,
- deterministic replay and time-travel,
- entity/version-aware workflows when useful,
- pgwire as the main application-facing runtime,
- optional clustered operation via pgwire + Raft.

ASQL is not a vertical product for healthcare, finance, or manufacturing. It should remain a compact database product with strong temporal and deterministic primitives that applications can build on.

## Product principles (non-negotiable)
1. **Single-node first**: every feature must work locally before clustered mode matters.
2. **Determinism first**: same WAL input must produce the same state and observable query results.
3. **Explicit boundaries**: domains and cross-domain work must remain visible.
4. **Append-only truth**: WAL is canonical; materialized state is derived.
5. **Observability by default**: metrics, logs, replay, and diagnostic visibility are part of the product.
6. **Minimal surface area**: add capabilities carefully; avoid accidental platform sprawl.
7. **General-purpose scope**: improve database primitives and ergonomics, not application-specific workflow semantics.

## Current product stance

Treat these as current realities unless the code or newer docs prove otherwise:

- canonical local runtime is `cmd/asqld` on pgwire,
- canonical Studio path is `asqlstudio` with `-pgwire-endpoint`,
- compatibility stance is a pragmatic PostgreSQL-compatible subset over pgwire,
- getting-started is the primary onboarding narrative,
- deeper docs should support getting-started rather than duplicate it.

## Architecture constraints
- Language: Go.
- Internal style: hexagonal / ports-and-adapters where practical.
- Keep core engine logic free from transport/framework coupling.
- No hidden global mutable state in execution paths.
- Deterministic abstractions for time/randomness must remain injectable in core code.

## Scope guidance

In scope:

- SQL/pgwire/database capabilities that are general-purpose,
- domain-scoped and cross-domain transaction semantics,
- replay, time-travel, entity/version primitives,
- fixture workflows, compatibility clarity, diagnostics, and tooling,
- operator-facing observability and production hardening.

Out of scope unless clearly reframed as database-general:

- workflow engines,
- approval systems,
- vertical compliance object models,
- domain-specific event taxonomies,
- business-specific case/timeline semantics.

Those belong in the application layer.

## Engineering standards
- Go formatting, tests, and determinism checks are mandatory.
- Public interfaces should include doc comments.
- Every state transition should be testable deterministically.
- New public capabilities should include integration tests.
- Update docs whenever user-visible behavior changes.

## Determinism checklist
- Does this change depend directly on wall-clock time?
- Does it depend on map iteration order?
- Does it introduce randomness without a controlled source?
- Does it rely on non-deterministic concurrency ordering?
- Can replay still produce identical state and results?

If any answer is “yes” to the first four, refactor before merging.

## Documentation rules
- Prefer improving `docs/getting-started/` over creating a parallel onboarding path.
- Keep `README.md` short; it is the front door, not the full guide.
- Use ADRs for durable architectural/product decisions.
- Mark stale planning docs as historical rather than silently letting them drift.
- Keep `.github/copilot-instructions.md` aligned with the current product state.
- Treat `README.md`, `docs/getting-started/`, `docs/reference/`, and `site/` as the primary external truth surfaces and keep them synchronized.
- Do not leave public SQL examples on syntax that the current parser rejects.
- Do not describe partial compatibility as full parity; use explicit supported/partial/unsupported language.
- When runtime paths differ (pgwire vs gRPC vs recovery/admin), state the canonical path first and demote secondary flows clearly.

## Agent workflow
1. Read the current task context and relevant docs.
2. Use `docs/ai/05-backlog.md` for active engineering execution unless the user asks for a docs/product audit task.
3. Implement the smallest useful vertical slice.
4. Add or update tests.
5. Update affected docs.
6. Record an ADR when the decision is durable and architectural.

## Definition of done
A task is done only if:

- code compiles,
- relevant tests pass,
- determinism constraints are preserved,
- observability/docs are updated where relevant,
- user-facing behavior is documented if it changed.

## Communication style for AI agents
- Be concise and explicit.
- Prefer incremental changes over large rewrites.
- Call out trade-offs and unresolved risks.
- Distinguish clearly between engine-owned concerns and app-owned concerns.
