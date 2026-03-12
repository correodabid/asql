# Definition of Done (ASQL)

## Task-level DoD
A task is done only if all apply:
- Implementation compiles.
- Relevant tests pass.
- Behavior is deterministic/replay-safe.
- Observability is present where relevant.
- Documentation is updated.

## Feature-level DoD
A feature is done only if:
- At least one integration test covers happy path.
- At least one failure-path test exists (conflict/corruption/error).
- User-facing API contract is documented.
- Backlog item is checked and linked to PR.

## Milestone-level DoD
A milestone is done only if:
- Exit criteria in roadmap are validated.
- Regression tests for previous milestones still pass.
- No unresolved P0/P1 determinism bugs remain.
- Release notes / changelog entry added.

## Determinism acceptance checks
- Re-running same WAL on clean state produces same state hash.
- Time-travel results are stable across repeated runs.
- Cross-domain transaction outcomes are independent of goroutine scheduling.
