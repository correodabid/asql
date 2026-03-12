# 08. Studio, CLI, and Daily Workflow

A practical ASQL adoption usually uses both Studio and `asqlctl`.

## Use Studio for exploration

Studio is best for:

- browsing schema,
- running ad hoc queries,
- inspecting row detail,
- viewing mutation history and entity history,
- inspecting temporal helper values,
- exploring snapshots and diffs in Time Explorer,
- fixture validate/load/export workflows.

Start it with:

```bash
go run ./cmd/asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

## Use `asqlctl` for scripts and repeatable commands

`asqlctl` is best for:

- shell automation,
- CI or smoke tests,
- scripted transactions,
- fixture workflows,
- reproducible team instructions.

## Guided CLI workflow: explain one state transition

When a team is still learning ASQL, use `asqlctl shell` to walk one explanation path end-to-end instead of guessing from current state.

Start the shell:

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

Then follow this sequence:

1. inspect the current row or root aggregate,
2. inspect the relevant temporal token,
3. inspect history,
4. reconstruct the historical snapshot,
5. inspect adoption signals if the workflow feels awkward.

Example:

```sql
SELECT id, status FROM billing.invoices WHERE id = 'inv-1';
SELECT row_lsn('billing.invoices', 'inv-1');
SELECT * FROM billing.invoices FOR HISTORY WHERE id = 'inv-1';
SELECT id, status, total_cents FROM billing.invoices AS OF LSN 12 WHERE id = 'inv-1';

SELECT
	total_begins,
	total_cross_domain_begins,
	cross_domain_begin_avg_domains,
	cross_domain_begin_max_domains,
	total_time_travel_queries
FROM asql_admin.engine_stats;
```

For aggregate-oriented workflows, add:

```sql
SELECT *
FROM asql_admin.entity_version_history
WHERE domain = 'billing'
	AND entity = 'invoice_aggregate'
	AND root_pk = 'inv-1';
```

This is a good default CLI path because it turns raw primitives into one repeatable explanation workflow.

## Suggested daily loop

1. run `asqld` locally,
2. use Studio for interactive inspection,
3. use `asqlctl` for scripted flows,
4. create fixtures for realistic scenarios,
5. verify temporal behavior with Time Explorer and helper queries.

## Recommended team habits

- keep a small stable fixture pack in the repository,
- document domain boundaries early,
- use Time Explorer for debugging instead of guessing from current state,
- prefer explicit transaction flows in examples and scripts,
- review adoption-oriented signals such as cross-domain breadth, time-travel usage, and entity-version history while the model is still evolving.

For a more explicit interpretation of these signals, see [../reference/adoption-signals-v1.md](../reference/adoption-signals-v1.md).

## Next step

Continue with [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md).
