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

For many teams, fixtures should arrive before service or UI integration.

Recommended sequence:

1. identify the first real domains,
2. create one small deterministic fixture for one realistic workflow,
3. validate it until it loads cleanly,
4. inspect the resulting state with SQL and Studio,
5. only then wire the same workflow into service code.

This is usually a better adoption path than starting with repository code or API handlers, because fixtures expose:

- domain-boundary mistakes,
- ordering mistakes,
- schema gaps,
- and temporal/reference assumptions,

before those problems are spread across application code.

## Fixture format

ASQL fixtures are strict JSON scenario files.
They contain ordered transaction steps with:

- a mode: `domain` or `cross`,
- participating domains,
- ordered SQL statements.

That structure is intentional: fixtures rehearse the same domain and transaction choices your application will make at runtime.

Reference:

- [../reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)

## Validate a fixture

```bash
go run ./cmd/asqlctl -command fixture-validate \
  -fixture-file path/to/your-fixture.json
```

Validation includes:

- spec validation,
- non-determinism checks,
- dry-run execution on a fresh ephemeral engine.

## How to read validation failures

Treat fixture validation as modeling feedback, not just syntax feedback.

Common failure categories usually mean:

### Non-deterministic token found

Typical cause:

- `NOW()`, `CURRENT_TIMESTAMP`, `RANDOM()`, generated IDs, or other runtime-derived values.

What to do:

- replace them with explicit IDs and timestamps,
- make ordering and data values reviewable in the fixture itself.

### Transaction-control statement found inside a step

Typical cause:

- carrying over seed SQL that still contains `BEGIN`, `COMMIT`, or `ROLLBACK`.

What to do:

- remove transaction control from statements,
- let the fixture step own transaction boundaries.

### Domain or dependency ordering failure

Typical cause:

- schema or seed statements are in the wrong order,
- a referenced table or row has not been created yet,
- a cross-domain step is missing one participating domain.

What to do:

- split the scenario into smaller ordered steps,
- move parent schema/data earlier,
- make the transaction scope match the actual invariant.

### Versioned-reference resolution failure

Typical cause:

- the referenced row or entity is not visible at the point of capture,
- the wrong table is modeled as an entity root,
- the scenario needs row-based semantics but is using entity-style expectations, or vice versa.

What to do:

- check whether the referenced table should be an entity,
- check whether the referenced mutation happens earlier in the same transaction or an earlier committed step,
- verify whether you want entity version capture or row-head `LSN` capture.

### Dry-run execution failure on a fresh engine

Typical cause:

- the fixture depends on external preloaded state,
- hidden assumptions from local dev data leaked into the scenario,
- the schema path is incomplete.

What to do:

- make the fixture self-contained,
- include every required schema and seed dependency in ordered steps,
- re-run validation before loading into a live server.

## Load a fixture

```bash
go run ./cmd/asqlctl -command fixture-load \
  -pgwire 127.0.0.1:5433 \
  -fixture-file path/to/your-fixture.json
```

## Export a fixture

```bash
go run ./cmd/asqlctl -command fixture-export \
  -pgwire 127.0.0.1:5433 \
  -domains billing,patients,clinical \
  -fixture-file path/to/your-export.json
```

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

Do not port loose seed scripts directly and expect the same result.
ASQL fixtures are intentionally stricter:

- no hidden wall-clock values,
- no randomness,
- no transaction control inside steps,
- no ambiguous dependency ordering.

That strictness is part of the product, not a temporary limitation.

## Adoption tip

Create one fixture per important workflow, not one giant environment dump.
Small scenarios are easier to reason about, review, replay, and evolve with the schema.

## Next step

Continue with [08-studio-cli-and-daily-workflow.md](08-studio-cli-and-daily-workflow.md).
