# 08. Studio, CLI, and Daily Workflow

A practical ASQL adoption usually uses both Studio and `asqlctl`.

## Use Studio for exploration

Studio is best for:

- starting from the guided `Start Here` overview,
- browsing schema,
- running ad hoc queries,
- inspecting row detail,
- viewing mutation history and entity history,
- inspecting temporal helper values,
- exploring snapshots and diffs in Time Explorer,
- fixture validate/load/export workflows.

Treat [asqlstudio](../../asqlstudio) as a public product surface: it consumes pgwire plus stable admin and fixture contracts rather than engine-private packages.

Start it with:

```bash
go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

If you need to point Studio at a different ASQL node later, use the runtime
connection manager from the title bar or status bar instead of relaunching the
desktop app. That flow can swap pgwire/admin endpoints and reuse the currently
stored tokens unless you enter replacements explicitly, and it keeps recent
connection targets so you can jump back to earlier nodes quickly, including a
one-click reconnect path for previously used endpoints.

After launch, the most useful first-run surfaces are usually:

- `Workspace` for pgwire SQL and explicit transaction controls,
- `Time Explorer` for history and snapshot comparison,
- `Fixtures` for deterministic sample data workflows,
- `Dashboard`, `Cluster`, and `Recovery` when you need operator-facing visibility.

## Use `asqlctl` for scripts and repeatable commands

`asqlctl` is best for:

- shell automation,
- CI or smoke tests,
- scripted transactions,
- fixture workflows,
- audit evidence export and retention review,
- reproducible team instructions.

The default day-to-day path in this guide is still pgwire-first:

- use `asqlctl shell` for interactive SQL over pgwire,
- use fixture commands for deterministic setup,
- use Studio for inspection.

Some `asqlctl` commands target lower-level admin or recovery surfaces instead of
the pgwire shell path. Treat those as advanced/operator flows rather than part
of the first-run local loop.

## Bootstrap and rotate database principals

When you enable durable principals, keep two layers separate:

- operator tokens still protect admin/process surfaces,
- database principals (`USER` / `ROLE`) govern pgwire login and in-database authorization.

The first admin principal is a one-time bootstrap step allowed only while the
durable principal catalog is empty.

Typical local sequence:

1. start `asqld` with the operator/admin token you want to use for bootstrap,
2. bootstrap the first admin principal,
3. use that durable principal for pgwire login,
4. rotate passwords through the durable-principal workflow rather than by
   changing the operator token.

Bootstrap from the CLI:

```bash
printf 'admin-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token write-secret \
	-password-stdin \
	-command principal-bootstrap-admin \
	-principal admin
```

Rotate a user password with the ergonomic alias path:

```bash
printf 'rotated-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token write-secret \
	-password-stdin \
	security user alter analyst
```

Studio exposes the same lifecycle through the `Security` area:

- bootstrap the first admin when no principal catalog exists,
- create users and roles,
- grant `SELECT_HISTORY` explicitly,
- inspect effective permissions,
- review recent denied authz checks and recent security-relevant changes.

Use the durable-principal path for database access changes. Do not treat
operator token rotation as a substitute for user/password rotation.

For the full security model, privilege semantics, and audit rules, see
[../reference/database-security-model-v1.md](../reference/database-security-model-v1.md).
That reference also includes copy/pasteable create/grant/revoke/denied-path
examples for the current durable-principal workflow.

Example: validate and load a fixture from the CLI.

```bash
go run ./cmd/asqlctl -command fixture-validate \
	-fixture-file fixtures/healthcare-billing-demo-v1.json

go run ./cmd/asqlctl -command fixture-load \
	-pgwire 127.0.0.1:5433 \
	-fixture-file fixtures/healthcare-billing-demo-v1.json
```

Schema-evolution review commands such as `migration-preflight` still exist in
`asqlctl`, but they belong to a lower-level admin path rather than the default
pgwire-first onboarding flow used in this chapter.

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

When the durable principal catalog is enabled, treat the `asql_admin` helpers as explicit security surfaces: `asql_admin.engine_stats` and other operator/admin views require `ADMIN`, while historical helpers such as `asql_admin.entity_version_history` and `asql_admin.row_history` require `SELECT_HISTORY`.

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
