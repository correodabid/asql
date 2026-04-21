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

If the team needs a shorter modeling aid, re-read the domain modeling checklist in [04-domains-and-transactions.md](04-domains-and-transactions.md) before splitting further.

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

- [../migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md)
- [12-first-postgres-service-flow.md](12-first-postgres-service-flow.md)

## Common adoption mistakes

:::warning[Patterns that slow every team down]
- Treating domains as mere schema prefixes instead of boundaries.
- Introducing cross-domain transactions too early.
- Using raw `LSN`s in app code when helper surfaces or entity versions are clearer.
- Leaving demo/test data as ad hoc SQL instead of fixtures.
- Expecting ASQL to absorb business workflow or compliance semantics that should stay in the application.
- Postponing history/time-travel until after the first incident.
:::

## Adoption FAQ for SQLite/Postgres/ORM-centric teams

:::details[Is ASQL basically PostgreSQL with a few extra features?]
**No.**

The pgwire runtime is intentionally PostgreSQL-shaped, but the product model is different in important ways:

- Domains are explicit.
- Temporal inspection is first-class.
- Fixtures are deterministic scenario assets.
- Replay is part of the normal mental model.

Treat ASQL as its own product with a pragmatic PostgreSQL-compatible subset, not as drop-in PostgreSQL equivalence.
:::

:::details[Do I really need domains for a small app?]
**Usually yes — but often only one at first.**

The mistake is not _"starting with one domain"_. The mistake is pretending boundaries do not exist at all.

For a narrow first rollout, one explicit domain is often the right move. Split later only when a second boundary is clearly justified.
:::

:::details[Should every multi-table workflow become `BEGIN CROSS DOMAIN ...`?]
**No.**

Use cross-domain scope only when atomicity across those domains is truly required. If the workflow is mainly orchestration, sequencing, or UI flow, that usually belongs in the application layer.
:::

:::details[Should I model entities immediately?]
**Only when the application already thinks in aggregates with a stable root and lifecycle.**

If the model is still fuzzy, start with rows, history, and fixtures first. Then add entities once the aggregate boundary is clear.
:::

:::details[Can I keep using my ORM exactly as it is today?]
**Usually not without adaptation.**

The biggest mismatch is not only SQL syntax — it is that ASQL expects explicit transaction boundaries and clearer ownership of workflow semantics.

If your ORM assumes hidden transaction flow, invisible cross-boundary writes, or full PostgreSQL behavior, expect integration work.

The current recommended compromise is a narrow ORM-lite lane:

- Keep SQL inspectable.
- Use `simple_protocol` first.
- Translate bare PostgreSQL transaction opens to explicit ASQL transaction primitives.
- Avoid assuming `UPDATE ... RETURNING`, `DELETE ... RETURNING`, arrays, or full catalog parity.

Use [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md) as the exact contract for that path. Use [../reference/postgres-app-sql-translation-guide-v1.md](../reference/postgres-app-sql-translation-guide-v1.md) for the next high-return SQL rewrites teams usually need during real app evaluation.

If the team wants a concrete first proof for an existing PostgreSQL-oriented Go service, use [12-first-postgres-service-flow.md](12-first-postgres-service-flow.md) before widening the adoption scope.
:::

:::details[Why does ASQL push fixtures so early?]
**Because fixtures expose adoption problems early:**

- Unclear domains.
- Ordering mistakes.
- Hidden runtime-generated values.
- Incorrect entity or reference assumptions.

They are often a faster way to harden the model than starting from handlers or UI paths.
:::

:::details[Should compliance or approval meaning live in ASQL?]
**Normally no.**

ASQL should help store facts, history, temporal references, and deterministic audit-friendly state. The application should still own:

- Approval meaning.
- Actor semantics.
- Evidence interpretation.
- Business-specific workflow rules.
:::

:::details[If ASQL speaks pgwire, can any PostgreSQL client/tool just work?]
**No.**

The correct assumption is:

- Many PostgreSQL-oriented tools can work within the documented compatibility surface.
- But teams should verify client behavior against the supported subset instead of assuming full PostgreSQL parity.

When in doubt, start with pgwire plus `pgx`, then validate additional tooling intentionally.

For the practical driver/query-mode recommendations, see [../reference/pgwire-driver-guidance-v1.md](../reference/pgwire-driver-guidance-v1.md).

For read-only dashboards, prefer one explicit custom-SQL panel first rather than builder-mode assumptions. Use [../reference/bi-lite-adoption-lane-v1.md](../reference/bi-lite-adoption-lane-v1.md) as the narrow contract for that path.
:::

## Where adoption friction usually appears

:::info[Surface these early — they are design work, not blockers]
- First domain split.
- First cross-domain workflow.
- First entity and versioned reference.
- First deterministic fixture pack.
- First question about whether a compliance concept belongs in ASQL or in app code.
:::

If the team is unsure whether a friction should become engine work, docs, SDK helpers, or tooling, use [../reference/adoption-review-rubric-v1.md](../reference/adoption-review-rubric-v1.md).

## Team checklist

- [x] One local startup command is documented
- [x] Domain boundaries are named and explained
- [x] First schema path is documented
- [x] Fixtures exist for realistic scenarios
- [x] `FOR HISTORY` and time-travel are part of debugging workflow
- [x] Studio is part of onboarding
- [x] One application integration example exists

## Compact starter conventions

Use this as the default app-side baseline unless the product has a strong reason to do something else.

### IDs

- use explicit stable IDs in fixtures and tests,
- prefer application-generated IDs over hidden database generation,
- keep one consistent ID format per aggregate family.

### Timestamps

- keep business timestamps explicit and app-owned,
- use literal timestamps in fixtures,
- use ASQL temporal helpers for replay-safe explanation, not as a substitute for business time semantics.

### Audit metadata

Recommended generic fields:

- `actor_id`
- `reason`
- `artifact_type`
- `artifact_id`
- `occurred_at`
- `payload_json`

### Transaction helpers

- keep `DOMAIN` vs `CROSS DOMAIN` decisions explicit,
- wrap boilerplate in small helpers,
- do not hide boundary decisions inside generic repositories.

### Temporal helpers

- use `current_lsn()`, `row_lsn(...)`, `entity_version(...)`, `entity_version_lsn(...)`, and `FOR HISTORY` in explicit helper functions,
- keep “current view + historical explanation” as one named application pattern.

### Fixture-first workflow

1. create one small fixture per important workflow,
2. validate it before wiring handlers,
3. reuse it in integration tests and demo/debug flows when practical.

## Next step

Continue with [11-troubleshooting.md](11-troubleshooting.md).
