# 11. Troubleshooting

Most ASQL onboarding issues come from one of four places:

- wrong endpoint or runtime assumption,
- unclear domain scope,
- missing deterministic setup,
- or confusion between engine-owned behavior and app-owned behavior.

## `connection refused`

Check:

- `asqld` is running,
- the endpoint is correct,
- Studio points to the same pgwire endpoint.

For local onboarding, the canonical default is `127.0.0.1:5433`.

If the issue is not connectivity but temporal or modeling confusion, use [../reference/temporal-modeling-troubleshooting-v1.md](../reference/temporal-modeling-troubleshooting-v1.md).

## `table not found`

Usually one of these is true:

- schema was never created,
- you are in the wrong domain,
- the query assumes implicit context that has not been established.

If this keeps recurring, revisit the first schema and fixture steps before adding more code.

## history output looks different than expected

Use the stable metadata names:

- `__operation`
- `__commit_lsn`

Do not rely on older internal field names.

Also confirm you are using `FOR HISTORY` against the intended qualified table name.

## versioned reference write fails

Check:

- the referenced row is visible,
- the referenced table is the entity root when using entity semantics,
- the reference is not pointing to a missing historical token.

If the data model is still changing quickly, simplify the workflow first and reintroduce versioned references once the aggregate boundary is stable.

To distinguish row-based capture from entity-based capture, compare `resolve_reference(...)`, `row_lsn(...)`, and `entity_version(...)` for the same target.

## fixture validation fails

Typical reasons:

- missing dependency order,
- references to data not yet inserted,
- unsupported non-deterministic tokens such as `NOW()` or `RANDOM()`,
- transaction control statements embedded inside fixture steps.

Treat fixture failures as useful design feedback, not just syntax errors.

## fixture export fails

The export path is intentionally strict.
Common causes:

- exported table has no primary key,
- selected domains omit dependency domains,
- dependency cycle prevents deterministic export.

## historical query confusion

Recommended sequence:

1. inspect `current_lsn()`,
2. inspect `row_lsn(...)` or `entity_version(...)`,
3. inspect `FOR HISTORY`,
4. run the exact `AS OF LSN` query.

## cross-domain workflow feels awkward

That usually means one of these is true:

- the boundary split is premature,
- the workflow is really application orchestration instead of atomic engine work,
- or the audit/compliance meaning has not been modeled clearly in application code.

Try rewriting the workflow in plain language first: what must commit together, and why?

Then inspect the cross-domain counters in `asql_admin.engine_stats` to see whether the model is drifting toward overly broad transaction scope.

## Go integration feels too manual

That is often normal at first.
ASQL prefers explicit orchestration over hidden repository behavior.
Start with a thin helper around `BEGIN DOMAIN ...` or `BEGIN CROSS DOMAIN ...`, then standardize request IDs, timestamps, and audit payload construction in the service layer.

## ORM or query-builder integration fails early

Usually one of these is true:

- the layer emitted bare `BEGIN` or `START TRANSACTION`,
- it assumed `UPDATE ... RETURNING` or `DELETE ... RETURNING`,
- it emitted arrays / `ANY(...)`,
- or it relied on broader PostgreSQL metadata behavior than the documented subset.

Start by proving the same workflow with literal SQL over pgwire and the exact connection settings from [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md).
If the literal-SQL path works, the remaining issue is usually in the abstraction layer rather than the engine semantics.

## where to look next

- [README.md](../../README.md)
- [02-install-and-run.md](02-install-and-run.md)
- [03-first-database.md](03-first-database.md)
- [10-adoption-playbook.md](10-adoption-playbook.md)
- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [../reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)
- [../reference/temporal-introspection-surface-v1.md](../reference/temporal-introspection-surface-v1.md)
- [../reference/temporal-modeling-troubleshooting-v1.md](../reference/temporal-modeling-troubleshooting-v1.md)
- [../reference/adoption-signals-v1.md](../reference/adoption-signals-v1.md)
