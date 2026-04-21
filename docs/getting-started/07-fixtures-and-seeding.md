# 07. Fixtures and Seeding

Fixtures are the recommended way to create reproducible demo, test, and benchmark data in ASQL.

## Why fixtures matter in ASQL

Because ASQL is deterministic, reproducible setup is especially valuable for:

- demos,
- integration tests,
- benchmark baselines,
- debugging from known initial state.

Fixtures are also one of the fastest ways to expose adoption friction.
If a scenario is hard to encode deterministically, the modeling or workflow boundaries are usually still unclear.

## Recommended onboarding order

:::tip[Fixtures before service or UI integration]
For many teams, fixtures should arrive **before** service or UI integration.
:::

:::steps
1. **Identify the first real domains.** Know what boundaries exist before encoding scenarios.
2. **Create one small deterministic fixture** for one realistic workflow.
3. **Validate it** until it loads cleanly on a fresh engine.
4. **Inspect the resulting state** with SQL and Studio.
5. **Only then wire the same workflow into service code.**
:::

This is usually a better adoption path than starting with repository code or API handlers, because fixtures expose **domain-boundary mistakes, ordering mistakes, schema gaps, and temporal/reference assumptions** before those problems are spread across application code.

## Fixture format

ASQL fixtures are strict JSON scenario files.
They contain ordered transaction steps with:

- a mode: `domain` or `cross`,
- participating domains,
- ordered SQL statements.

That structure is intentional: fixtures rehearse the same domain and transaction choices your application will make at runtime.

Reference:

- [../reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)

## Fixture lifecycle commands

:::tabs
:::tab[Validate]
```bash
go run ./cmd/asqlctl -command fixture-validate \
  -fixture-file path/to/your-fixture.json
```

Validation includes **spec validation, non-determinism checks, and dry-run execution on a fresh ephemeral engine**.
:::tab[Load]
```bash
go run ./cmd/asqlctl -command fixture-load \
  -pgwire 127.0.0.1:5433 \
  -fixture-file path/to/your-fixture.json
```

Loads a validated fixture into a running ASQL engine.
:::tab[Export]
```bash
go run ./cmd/asqlctl -command fixture-export \
  -pgwire 127.0.0.1:5433 \
  -domains billing,patients,clinical \
  -fixture-file path/to/your-export.json
```

Exports a deterministic fixture file from the current state of one or more domains.
:::

## How to read validation failures

:::note[Validation failures are modeling feedback]
Treat fixture validation as **modeling feedback, not just syntax feedback**. If a scenario is hard to encode deterministically, the boundaries or ordering are usually still unclear.
:::

:::details[Non-deterministic token found]
**Typical cause**

- `NOW()`, `CURRENT_TIMESTAMP`, `RANDOM()`, generated IDs, or other runtime-derived values.

**What to do**

- Replace them with explicit IDs and timestamps.
- Make ordering and data values reviewable in the fixture itself.
:::

:::details[Transaction-control statement found inside a step]
**Typical cause**

- Carrying over seed SQL that still contains `BEGIN`, `COMMIT`, or `ROLLBACK`.

**What to do**

- Remove transaction control from statements.
- Let the fixture step own transaction boundaries.
:::

:::details[Domain or dependency ordering failure]
**Typical cause**

- Schema or seed statements are in the wrong order.
- A referenced table or row has not been created yet.
- A cross-domain step is missing one participating domain.

**What to do**

- Split the scenario into smaller ordered steps.
- Move parent schema/data earlier.
- Make the transaction scope match the actual invariant.
:::

:::details[Versioned-reference resolution failure]
**Typical cause**

- The referenced row or entity is not visible at the point of capture.
- The wrong table is modeled as an entity root.
- The scenario needs row-based semantics but is using entity-style expectations, or vice versa.

**What to do**

- Check whether the referenced table should be an entity.
- Check whether the referenced mutation happens earlier in the same transaction or an earlier committed step.
- Verify whether you want entity version capture or row-head `LSN` capture.
:::

:::details[Dry-run execution failure on a fresh engine]
**Typical cause**

- The fixture depends on external preloaded state.
- Hidden assumptions from local dev data leaked into the scenario.
- The schema path is incomplete.

**What to do**

- Make the fixture self-contained.
- Include every required schema and seed dependency in ordered steps.
- Re-run validation before loading into a live server.
:::

## Studio workflow

Studio provides a dedicated `Fixtures` tab for:

- picking a fixture file,
- validating it,
- loading it,
- exporting selected domains into a deterministic fixture file.

## Best practices

- keep IDs and timestamps explicit,
- treat fixtures as scenario assets, not just table dumps,
- prefer small, meaningful business scenarios,
- validate fixtures before commit.

## Common expectation mismatch

:::warning[Do not port loose seed scripts directly]
ASQL fixtures are intentionally stricter than ad-hoc seed files:

- No hidden wall-clock values.
- No randomness.
- No transaction control inside steps.
- No ambiguous dependency ordering.

That strictness is **part of the product, not a temporary limitation**.
:::

## Adoption tip

:::tip[One fixture per workflow, not one giant dump]
Create one fixture per important workflow, not one giant environment dump. Small scenarios are easier to reason about, review, replay, and evolve with the schema.
:::

## Next step

Continue with [08-studio-cli-and-daily-workflow.md](08-studio-cli-and-daily-workflow.md).
