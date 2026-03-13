# Definition of Done (ASQL)

Status note (2026-03-12): this remains current. Pair it with [docs/ai/05-backlog.md](05-backlog.md) for active execution tracking and [.github/copilot-instructions.md](../../.github/copilot-instructions.md) for product/runtime constraints.

## Task-level DoD
A task is done only if all apply:
- Implementation compiles.
- Relevant tests pass.
- Behavior is deterministic/replay-safe.
- Observability is present where relevant.
- Documentation is updated.
- Any changed public examples use currently supported syntax.
- Any changed public claim is backed by current code, tests, or explicitly marked as partial.

## Feature-level DoD
A feature is done only if:
- At least one integration test covers happy path.
- At least one failure-path test exists (conflict/corruption/error).
- User-facing API contract is documented.
- User-facing docs/reference/site surfaces are aligned with the implemented behavior.
- Backlog item is checked and linked to PR.

## Milestone-level DoD
A milestone is done only if:
- Exit criteria in roadmap are validated.
- Regression tests for previous milestones still pass.
- No unresolved P0/P1 determinism bugs remain.
- Release notes / changelog entry added.
- No known documentation drift remains on primary adoption surfaces (`README.md`, `docs/getting-started/`, `docs/reference/`, `site/`).

## Documentation acceptance checks
- `README.md` still matches the canonical runtime and onboarding path.
- `docs/getting-started/` remains the primary happy path and does not defer to lower-level flows by accident.
- `docs/reference/` distinguishes clearly between supported, partial, and unsupported behavior.
- Public site examples use syntax accepted by the current parser/runtime.
- Stale planning material is marked historical or moved under `docs/legacy/`.

## Determinism acceptance checks
- Re-running same WAL on clean state produces same state hash.
- Time-travel results are stable across repeated runs.
- Cross-domain transaction outcomes are independent of goroutine scheduling.
