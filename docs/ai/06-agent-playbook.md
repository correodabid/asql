# Agent Playbook

Status note (2026-03-12): this remains current as the lightweight execution loop for internal agents.
Use [docs/ai/05-backlog.md](05-backlog.md) as the active task source and [.github/copilot-instructions.md](../../.github/copilot-instructions.md) for current runtime/product constraints.

## Daily execution loop for AI agents
1. Read `docs/ai/01-product-vision.md`.
2. Select one unchecked item from `docs/ai/05-backlog.md`.
3. Confirm assumptions in task notes.
4. Implement minimal code to satisfy the task.
5. Add/update unit/integration tests.
6. Run local checks.
7. Update backlog status and docs.

## Required checks before marking task done
- `go test ./...`
- `go vet ./...`
- formatting check (`gofmt`)
- determinism checklist from `.github/copilot-instructions.md`

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
