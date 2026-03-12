# 10. Adoption Playbook

This guide is for teams moving an existing app toward ASQL.

## Recommended rollout order

### Phase 1: local single-node adoption

Start with:

- one local ASQL instance,
- one or two domains,
- ordinary writes and reads,
- a small fixture pack.

Goal: prove the application model, not clustering.

### Phase 2: make domains explicit

Identify the first real boundaries in the app, for example:

- users vs billing,
- patients vs clinical vs billing,
- catalog vs orders vs inventory.

Do not over-split early.
Start with boundaries the team already understands.

### Phase 3: adopt temporal workflows

Add:

- `FOR HISTORY` for mutation debugging,
- `AS OF LSN` reads for reproducible analysis,
- helper functions for targeted introspection.

### Phase 4: adopt entities where the model needs aggregates

Add entity definitions where the business model already thinks in aggregates.
Do not force entities onto every table.

### Phase 5: production hardening

Only after the single-node model is solid should you expand into broader operational patterns.

## Migration from SQLite-style workloads

If the current workload is close to SQLite in shape:

- start with one domain,
- keep SQL simple,
- replace implicit write flows with explicit transaction orchestration,
- use fixtures to reproduce representative data.

Related docs:

- [../migration-sqlite-quick-path.md](../migration-sqlite-quick-path.md)
- [../migration-sqlite-postgres-lite-guide-v1.md](../migration-sqlite-postgres-lite-guide-v1.md)

## Common adoption mistakes

- treating domains as mere schema prefixes instead of boundaries,
- introducing cross-domain transactions too early,
- using raw `LSN`s in app code when helper surfaces or entity versions are clearer,
- leaving demo/test data as ad hoc SQL instead of fixtures.

## Team checklist

- [ ] One local startup command is documented
- [ ] Domain boundaries are named and explained
- [ ] First schema path is documented
- [ ] Fixtures exist for realistic scenarios
- [ ] `FOR HISTORY` and time-travel are part of debugging workflow
- [ ] Studio is part of onboarding
- [ ] One application integration example exists

## Next step

Continue with [11-troubleshooting.md](11-troubleshooting.md).
