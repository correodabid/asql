# ASQL adoption friction prioritized backlog v1

## Purpose

Turn the general adoption-friction findings into a prioritized backlog that improves ASQL as a general-purpose database product without pulling app-specific workflow semantics into the engine.

## Boundary rule

This backlog follows one product rule:

- improve engine-general primitives, ergonomics, tooling, and guidance,
- do not move business workflow ownership from the application into ASQL unless the capability is clearly database-general.

## Documentation and examples delivery rule

Documentation and example work for this backlog should preferentially improve the getting-started path instead of creating scattered standalone documents.

That means:

- new user-facing guidance should land in [docs/getting-started/](getting-started),
- example flows should be wired into the getting-started progression,
- cookbook and example-app material should support the onboarding path rather than compete with it,
- standalone docs should be used only when the topic is too large, too reference-heavy, or too architectural for getting-started.

Practical rule:

- if a new user should learn it in the first adoption journey, it belongs in getting-started,
- if a returning team needs it as a policy/reference artifact, it may live outside getting-started.

## Non-goals

The backlog does not aim to make ASQL directly own:

- workflow engines,
- approval systems,
- e-signature policy catalogs,
- role/actor business models,
- case-management timelines,
- vertical compliance object models.

Those belong in the application layer, supported by ASQL primitives where useful.

## Prioritization model

- `P0`: adoption blocker or expectation mismatch affecting many use cases
- `P1`: high leverage for realistic applications, SDKs, examples, and onboarding
- `P2`: strong productization improvement after baseline ergonomics are in place
- `P3`: strategic follow-on work

## Epic AC — Responsibility boundaries and expectation-setting (`P0`)

### Goal

Make it obvious what ASQL owns and what the application owns.

### Tasks

- [ ] Publish a short product note: engine-owned vs app-owned concerns.
  - Include domains, temporal reads, replay, history, entity/versioning as engine concerns.
  - Include workflow policies, approval semantics, compliance vocabularies, and actor semantics as app concerns.
  - Definition of done:
    - note exists
    - getting-started points to it or absorbs its content directly
    - README and getting-started docs link to the same source of truth

- [ ] Add “responsibility boundary” callouts to examples and guides.
  - Definition of done:
    - at least getting-started and one example app include explicit labels
    - no parallel onboarding narrative exists outside getting-started

- [ ] Add feature-triage rule for roadmap planning.
  - For each proposed feature ask: is this database-general or app-specific?
  - Definition of done:
    - triage rubric documented

## Epic AD — Domain modeling and transaction ergonomics (`P0`)

### Goal

Reduce the friction of explicit domains and explicit transaction scopes without weakening them.

### Tasks

- [ ] Publish a concise domain modeling guide.
  - Focus on how to find boundaries, when not to split, and how to avoid cross-domain sprawl.
  - Definition of done:
    - examples cover at least three different industries
    - guide is integrated into getting-started or adoption-playbook flow

- [ ] Add application integration patterns for domain-scoped transaction helpers.
  - Prefer Go-first examples, with patterns reusable by other SDKs later.
  - Definition of done:
    - reference helper pattern documented
    - example code committed
    - getting-started links to the example at the right stage

- [ ] Add explicit diagnostics for cross-domain overuse.
  - Could be telemetry, logs, or lint-like guidance in tooling.
  - Definition of done:
    - one supported visibility path exists

## Epic AE — Temporal workflow composition (`P1`)

### Goal

Make temporal features easier to consume in application-facing workflows without turning ASQL into a case-management product.

### Tasks

- [ ] Publish reference patterns for composing timelines from `FOR HISTORY`, `AS OF LSN`, and entity helpers.
  - Definition of done:
    - patterns documented for generic case/event history
    - getting-started temporal chapters are improved rather than duplicated elsewhere

- [ ] Add one SDK-oriented helper package or cookbook section for temporal query composition.
  - Keep helpers generic: snapshots, history fetch, version lookup, diff inputs.
  - Definition of done:
    - Go example committed
    - no workflow-specific semantics embedded in the helper

- [ ] Add example queries for “current view + historical explanation” workflows.
  - Definition of done:
    - docs include examples for at least row, entity, and multi-table snapshot reconstruction
    - examples appear in getting-started before any deeper reference doc

## Epic AF — Audit-friendly reference patterns (`P1`)

### Goal

Help teams build consistent app-owned audit layers on top of ASQL.

### Tasks

- [ ] Publish a generic audit metadata pattern.
  - Suggested fields: `actor_id`, `reason`, `artifact_type`, `artifact_id`, `occurred_at`, `payload_json`.
  - Definition of done:
    - guidance is clearly framed as recommended application pattern, not engine requirement
    - getting-started and examples explain the boundary explicitly

- [ ] Add one reference schema pack for generic auditable events.
  - Keep it product-neutral.
  - Definition of done:
    - example SQL and fixture included

- [ ] Add cookbook guidance on when to use raw history vs app-owned audit tables.
  - Definition of done:
    - trade-offs documented clearly
    - getting-started includes the short version and cookbook/reference holds the extended version

## Epic AG — Starter kits and opinionated defaults outside the core engine (`P1`)

### Goal

Lower early adoption cost with starter material rather than engine bloat.

### Tasks

- [ ] Create a general-purpose starter pack for app conventions.
  - IDs, timestamps, audit event shape, transaction helper patterns, fixture-first workflow.
  - Definition of done:
    - starter pack exists in docs or example app template
    - getting-started points to it as the preferred starting path for real apps

- [ ] Add one example app whose purpose is adoption learning, not vertical productization.
  - Current bank app can be one input, but the guidance must be generalized.
  - Definition of done:
    - example explicitly calls out which parts are app-owned
    - example is presented as an extension of getting-started, not a separate learning track

- [ ] Add a fixture-first onboarding flow to docs.
  - Definition of done:
    - onboarding path starts from deterministic scenario before API/UI layers
    - the primary landing zone is getting-started

## Epic AH — Compatibility and surprise reduction (`P1`)

### Goal

Reduce friction from incorrect assumptions about SQL/database behavior.

### Tasks

- [ ] Expand compatibility docs with “common expectation mismatches”.
  - Examples: implicit transactions, classic FK assumptions, ORM expectations, Postgres assumptions.
  - Definition of done:
    - docs contain mismatch list and recommended ASQL-native approach
    - short mismatch summary is reachable from getting-started and troubleshooting

- [ ] Add guardrail messaging for likely misuse patterns.
  - Definition of done:
    - at least one class of unsupported or risky usage gets actionable feedback

- [ ] Add adoption FAQ for teams coming from SQLite/Postgres/ORM-centric stacks.
  - Definition of done:
    - FAQ linked from getting-started and migration docs
    - duplicate explanations are removed from scattered docs where possible

## Epic AI — Observability for adoption (`P2`)

### Goal

Make it easier to see where teams are struggling during real adoption.

### Tasks

- [ ] Add telemetry or debug counters for transaction-scope usage.
  - Example: single-domain vs cross-domain counts.
  - Definition of done:
    - one supported visibility surface exists

- [ ] Add fixture validation error categories tuned for adoption learning.
  - Definition of done:
    - errors point developers to modeling or ordering issues explicitly

- [ ] Add optional developer diagnostics for temporal reference resolution.
  - Definition of done:
    - teams can inspect what the engine resolved without reading internal code

## Epic AJ — Reference architecture and training material (`P2`)

### Goal

Make adoption teachable without changing the engine surface too aggressively.

### Tasks

- [ ] Publish a reference architecture for “app + ASQL” separation of concerns.
  - Definition of done:
    - shows transaction orchestration, audit layer, timeline projections, and fixture workflow boundaries
    - getting-started links to this as the deeper follow-on after onboarding

- [ ] Add training material for the ASQL mental model.
  - Focus on determinism, domains, replay, temporal inspection, and aggregate semantics.
  - Definition of done:
    - one concise guide exists for engineering teams
    - the condensed version is embedded in getting-started

- [ ] Add design-partner interview checklist for adoption friction.
  - Definition of done:
    - feedback collection uses consistent categories

## Not recommended for this backlog

These ideas should be rejected unless reframed as clearly database-general:

- embedding an approval engine in ASQL,
- embedding vertical compliance models in the core engine,
- shipping healthcare/finance/manufacturing-specific schema semantics as product surface,
- turning ASQL entities into a domain workflow runtime,
- auto-modeling business timelines inside the engine.

## Suggested execution order

1. Epic AC — responsibility boundaries and expectation-setting
2. Epic AD — domain modeling and transaction ergonomics
3. Epic AH — compatibility and surprise reduction
4. Epic AF — audit-friendly reference patterns
5. Epic AG — starter kits and opinionated defaults outside the core engine
6. Epic AE — temporal workflow composition
7. Epic AI — observability for adoption
8. Epic AJ — reference architecture and training material

## Content routing guidance

When implementing documentation/example tasks in this backlog:

- use getting-started as the primary narrative spine,
- extend cookbook/reference docs only when more depth is needed,
- avoid creating multiple parallel onboarding stories,
- keep example applications subordinate to the learning path defined in getting-started.

## Acceptance gates

- [ ] docs clearly separate engine-owned and app-owned concerns
- [ ] adoption material helps teams model domains and transactions with less confusion
- [ ] app teams have reusable patterns for audit metadata without expecting ASQL to own workflow semantics
- [ ] fixture-first onboarding is documented and practical
- [ ] common expectation mismatches are documented and less surprising

## Related documents

- [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](adr/0002-generalist-engine-boundary-and-adoption-surface.md)
- [bankapp/FRICTION_LOG.md](../../bankapp/FRICTION_LOG.md)
- [docs/ai/05-backlog.md](ai/05-backlog.md)
