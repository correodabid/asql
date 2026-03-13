# Agent Playbook

Status note (2026-03-12): this remains current as the lightweight execution loop for internal agents.
Use [docs/ai/05-backlog.md](05-backlog.md) as the active task source and [.github/copilot-instructions.md](../../.github/copilot-instructions.md) for current runtime/product constraints.

## Daily execution loop for AI agents
1. Read `docs/ai/01-product-vision.md`.
2. Read `docs/ai/02-architecture-blueprint.md` when the task changes runtime or core-engine structure.
3. Select one unchecked item from `docs/ai/05-backlog.md`.
4. Confirm assumptions in task notes.
5. Implement minimal code to satisfy the task.
6. Add/update unit/integration tests.
7. Run local checks.
8. Update backlog status and docs.
9. Verify that public claims/examples still match code and tests.

## Documentation sync checklist
When a change affects externally visible behavior, agents should update the
smallest relevant doc surface instead of leaving drift behind.

Check all that apply:
- `README.md` if the front-door narrative, examples, or runtime path changed.
- `docs/getting-started/` if onboarding commands, Studio flow, or the happy path changed.
- `docs/reference/` if SQL syntax, compatibility claims, driver guidance, admin contracts, or fixture rules changed.
- `site/` if public marketing/examples mirror the changed behavior.
- `docs/operations/` if operator runbooks, recovery flows, metrics, or support expectations changed.
- `docs/product/` / `docs/commercial/` if roadmap or commercial narratives would otherwise overstate current reality.

Rules:
- Prefer tested examples over aspirational examples.
- If support is partial, say so explicitly.
- If a claim is no longer current, update it or mark the doc as historical.
- Do not introduce new PostgreSQL-parity claims without test evidence.

## Required checks before marking task done
- `go test ./...`
- `go vet ./...`
- formatting check (`gofmt`)
- determinism checklist from `.github/copilot-instructions.md`
- documentation/examples re-checked for syntax drift where relevant

## PR template for agents
```markdown
## Task
- Backlog item: <epic/task>

## What changed
- <concise bullets>

## Determinism
- [ ] No wall-clock dependency introduced
- [ ] No map iteration dependence in critical path
- [ ] No non-deterministic randomness
- [ ] Replay behavior verified

## Validation
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] `go test ./...` passes
- [ ] Public docs/examples updated where needed
- [ ] Claims checked against current code/tests

## Risks / follow-ups
- <if any>
```

## ADR trigger conditions
Create a new ADR when changing:
- WAL format,
- transaction protocol,
- replay guarantees,
- domain isolation model,
- replication consistency model.

## Suggested ADR filename
`docs/adr/YYYYMMDD-<short-title>.md`
