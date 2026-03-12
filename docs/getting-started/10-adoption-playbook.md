# 10. Adoption Playbook

This guide is for teams moving an existing app toward ASQL.

The goal is not to rewrite the application around database features.
The goal is to make boundaries, history, and deterministic setup explicit where they already matter.

## Recommended rollout order

### Phase 1: local single-node adoption

Start with:

- one local ASQL instance,
- one or two domains,
- ordinary writes and reads,
- a small fixture pack.

Goal: prove the application model, not clustering.

Expectation to set with the team: success in this phase means the first workflow is understandable and reproducible, not that the full platform story is finished.

### Phase 2: make domains explicit

Identify the first real boundaries in the app, for example:

- users vs billing,
- patients vs clinical vs billing,
- catalog vs orders vs inventory.

Do not over-split early.
Start with boundaries the team already understands.

If the team cannot explain why two concepts must commit together, they probably should not be separate domains yet.

### Phase 3: adopt temporal workflows

Add:

- `FOR HISTORY` for mutation debugging,
- `AS OF LSN` reads for reproducible analysis,
- helper functions for targeted introspection.

Add these early enough that developers actually use them during debugging.
If temporal workflows arrive too late, they get treated as niche features instead of normal practice.

### Phase 4: adopt entities where the model needs aggregates

Add entity definitions where the business model already thinks in aggregates.
Do not force entities onto every table.

Use them when the application already has a stable root and version semantics, not as a substitute for unclear modeling.

### Phase 5: production hardening

Only after the single-node model is solid should you expand into broader operational patterns.

This includes cluster concerns, more advanced observability, and stricter rollout controls.

## Migration from SQLite-style workloads

If the current workload is close to SQLite in shape:

- start with one domain,
- keep SQL simple,
- replace implicit write flows with explicit transaction orchestration,
- use fixtures to reproduce representative data.

The main mindset change is that ASQL makes boundaries visible instead of letting them stay implicit in repository code.

Related docs:

- [../migration/sqlite-quick-path.md](../migration/sqlite-quick-path.md)
- [../migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md)

## Common adoption mistakes

- treating domains as mere schema prefixes instead of boundaries,
- introducing cross-domain transactions too early,
- using raw `LSN`s in app code when helper surfaces or entity versions are clearer,
- leaving demo/test data as ad hoc SQL instead of fixtures,
- expecting ASQL to absorb business workflow or compliance semantics that should stay in the application,
- postponing history/time-travel until after the first incident.

## Where adoption friction usually appears

- first domain split,
- first cross-domain workflow,
- first entity and versioned reference,
- first deterministic fixture pack,
- first question about whether a compliance concept belongs in ASQL or in app code.

Surface those questions early. They are usually design work, not blockers.

## Team checklist

- [ ] One local startup command is documented
- [ ] Domain boundaries are named and explained
- [ ] First schema path is documented
- [ ] Fixtures exist for realistic scenarios
- [ ] `FOR HISTORY` and time-travel are part of debugging workflow
- [ ] Studio is part of onboarding
- [ ] One application integration example exists

## Reference examples

- [../../hospitalapp/README.md](../../hospitalapp/README.md)
- [../../hospitalapp/FRICTION_LOG.md](../../hospitalapp/FRICTION_LOG.md)
- [../product/asql-adoption-friction-prioritized-backlog-v1.md](../product/asql-adoption-friction-prioritized-backlog-v1.md)

## Next step

Continue with [11-troubleshooting.md](11-troubleshooting.md).
