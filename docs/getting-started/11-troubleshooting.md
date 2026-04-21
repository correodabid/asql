# 11. Troubleshooting

:::info[Most onboarding issues fall in four buckets]
- Wrong endpoint or runtime assumption.
- Unclear domain scope.
- Missing deterministic setup.
- Confusion between engine-owned behavior and app-owned behavior.
:::

:::details[`connection refused`]
**Check**

- `asqld` is running.
- The endpoint is correct.
- Studio points to the same pgwire endpoint.

For local onboarding, the canonical default is `127.0.0.1:5433`.

If the issue is not connectivity but temporal or modeling confusion, use [../reference/temporal-modeling-troubleshooting-v1.md](../reference/temporal-modeling-troubleshooting-v1.md).
:::

:::details[`table not found`]
Usually one of these is true:

- Schema was never created.
- You are in the wrong domain.
- The query assumes implicit context that has not been established.

If this keeps recurring, revisit the first schema and fixture steps before adding more code.
:::

:::details[History output looks different than expected]
Use the **stable** metadata names:

- `__operation`
- `__commit_lsn`

Do not rely on older internal field names.

Also confirm you are using `FOR HISTORY` against the intended qualified table name.
:::

:::details[Versioned reference write fails]
**Check**

- The referenced row is visible.
- The referenced table is the entity root when using entity semantics.
- The reference is not pointing to a missing historical token.

If the data model is still changing quickly, simplify the workflow first and reintroduce versioned references once the aggregate boundary is stable.

To distinguish row-based capture from entity-based capture, compare `resolve_reference(...)`, `row_lsn(...)`, and `entity_version(...)` for the same target.
:::

:::details[Fixture validation fails]
**Typical reasons**

- Missing dependency order.
- References to data not yet inserted.
- Unsupported non-deterministic tokens such as `NOW()` or `RANDOM()`.
- Transaction control statements embedded inside fixture steps.

Treat fixture failures as **useful design feedback**, not just syntax errors.
:::

:::details[Fixture export fails]
The export path is intentionally strict. Common causes:

- Exported table has no primary key.
- Selected domains omit dependency domains.
- Dependency cycle prevents deterministic export.
:::

:::details[Historical query confusion]
Recommended sequence:

1. Inspect `current_lsn()`.
2. Inspect `row_lsn(...)` or `entity_version(...)`.
3. Inspect `FOR HISTORY`.
4. Run the exact `AS OF LSN` query.
:::

:::details[Cross-domain workflow feels awkward]
That usually means one of these is true:

- The boundary split is premature.
- The workflow is really application orchestration instead of atomic engine work.
- The audit/compliance meaning has not been modeled clearly in application code.

Try rewriting the workflow in plain language first: **what must commit together, and why?**

Then inspect the cross-domain counters in `asql_admin.engine_stats` to see whether the model is drifting toward overly broad transaction scope.
:::

:::details[Go integration feels too manual]
That is often normal at first.

ASQL prefers explicit orchestration over hidden repository behavior. Start with a thin helper around `BEGIN DOMAIN ...` or `BEGIN CROSS DOMAIN ...`, then standardize request IDs, timestamps, and audit payload construction in the service layer.
:::

:::details[ORM or query-builder integration fails early]
Usually one of these is true:

- The layer emitted bare `BEGIN` or `START TRANSACTION`.
- It assumed `UPDATE ... RETURNING` or `DELETE ... RETURNING`.
- It emitted arrays / `ANY(...)`.
- It relied on broader PostgreSQL metadata behavior than the documented subset.

Start by proving the same workflow with literal SQL over pgwire and the exact connection settings from [../reference/orm-lite-adoption-lane-v1.md](../reference/orm-lite-adoption-lane-v1.md).

If the literal-SQL path works, the remaining issue is usually in the abstraction layer rather than the engine semantics.
:::

:::details[BI or dashboard datasource works partially]
Usually one of these is true:

- The tool is using builder-mode metadata queries beyond the documented shim subset.
- It expects macro expansion or time-series-specific PostgreSQL behavior not currently claimed.
- It assumes broader type or catalog parity than ASQL documents.
- It is trying to autocomplete tables/columns through tool-specific PostgreSQL helper functions.

Start with one explicit read-only custom-SQL panel and the connection settings in [../reference/bi-lite-adoption-lane-v1.md](../reference/bi-lite-adoption-lane-v1.md).

If that works, treat any remaining builder/autocomplete issue as tool-surface validation work rather than as an engine regression.
:::

## Where to look next

- [README.md](../../README.md)
- [02-install-and-run.md](02-install-and-run.md)
- [03-first-database.md](03-first-database.md)
- [10-adoption-playbook.md](10-adoption-playbook.md)
- [12-first-postgres-service-flow.md](12-first-postgres-service-flow.md)
- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [../reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)
- [../reference/temporal-introspection-surface-v1.md](../reference/temporal-introspection-surface-v1.md)
- [../reference/temporal-modeling-troubleshooting-v1.md](../reference/temporal-modeling-troubleshooting-v1.md)
- [../reference/adoption-signals-v1.md](../reference/adoption-signals-v1.md)
