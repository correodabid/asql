# 05. Time Travel, History, and Debugging

ASQL treats historical inspection as a normal developer workflow.

## Transport tokens vs database principals

Keep these two layers separate:

- transport and admin tokens are deployment/operator controls,
- durable database principals are engine-owned identities used for pgwire login and grant evaluation.

That means:

- `-auth-token`, `-admin-read-token`, and `-admin-write-token` protect process-level surfaces,
- durable principals (`USER` / `ROLE`) control who may run current reads, current mutations, and historical reads inside the database model,
- granting a database privilege does not replace operator tokens,
- rotating an operator token does not silently grant or revoke database privileges.

## Read a past snapshot

You can query past state by `LSN` directly over pgwire.

```sql
SELECT id, email FROM app.users AS OF LSN 4;
```

If you prefer the shell during onboarding:

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

Then run:

```sql
SELECT current_lsn();
SELECT id, email FROM app.users AS OF LSN 4;
```

There are lower-level admin and recovery commands elsewhere in the product, but
the normal developer path in getting-started should be pgwire queries and
Studio.

## Historical access authorization rule

:::warning[Authorization is always current-state]
When the durable principal catalog is enabled, historical reads are authorized against the **current** principal/grant state — not against a reconstructed historical security state.

- Ordinary `SELECT` is not enough for time-travel or history access.
- `AS OF LSN`, `AS OF TIMESTAMP`, and `FOR HISTORY` require the explicit `SELECT_HISTORY` privilege.
- The data snapshot may be old, but the authorization decision is made using the principal's **current** grants.
- ASQL does not pretend that a user or role existed in the past just because the query targets an old `LSN` or timestamp.
:::

Example consequence:

1. a principal is created today,
2. `SELECT_HISTORY` is granted today,
3. that principal can now query older snapshots,
4. audit output records the current authorization decision instead of inventing
	a backdated principal history.

Concrete example:

1. the row in `app.users` was written yesterday at `LSN 42`,
2. `late_reader` is created today,
3. `late_reader` initially cannot run `AS OF LSN 42`,
4. `late_reader` is granted `SELECT_HISTORY` today,
5. `late_reader` can now read `AS OF LSN 42`,
6. audit output shows that the read was authorized by the **current** grant,
	not by pretending `late_reader` existed yesterday.

This keeps the model deterministic and auditable:

- time-travel answers “what did the data look like then?”
- authorization answers “is this principal allowed to ask for that now?”

If your workflow needs ordinary current reads but not historical visibility,
grant current read access without granting `SELECT_HISTORY`.

## Supported temporal helper surface

These helpers are part of the supported product surface:

- `current_lsn()`
- `row_lsn('domain.table', 'pk')`
- `entity_version('domain', 'entity', 'root_pk')`
- `entity_head_lsn('domain', 'entity', 'root_pk')`
- `entity_version_lsn('domain', 'entity', 'root_pk', version)`
- `resolve_reference('domain.table', 'pk')`

## What each helper is for

### `current_lsn()`

Use it when you need the current committed head.

### `row_lsn(...)`

Use it when you need the current visible head `LSN` of one row.

### `entity_version(...)`

Use it when the table belongs to an entity aggregate and you want the current business-facing version.

### `entity_head_lsn(...)`

Use it when you want the commit `LSN` of the latest visible aggregate version.

### `entity_version_lsn(...)`

Use it when you already know an entity version and want the replay-safe `LSN` that corresponds to it.
This is the bridge between aggregate semantics and `AS OF LSN` reads.

### `resolve_reference(...)`

Use it to inspect what a versioned foreign key would capture right now.

## Common expectation mismatch

:::note[Temporal features are first-class SQL]
Do not treat temporal features as a separate admin subsystem. In ASQL they are part of the normal SQL workflow for debugging state transitions, comparing current and past reads, validating fixture outcomes, and explaining audit trails.
:::

## `FOR HISTORY`

ASQL exposes a stable `FOR HISTORY` contract.
The canonical metadata columns are:

- `__operation`
- `__commit_lsn`

Example:

```sql
SELECT * FROM app.users FOR HISTORY WHERE id = 1;
```

Use this when you want the chronological mutation trail for one row or a filtered set of rows.

When the durable principal catalog is enabled, `FOR HISTORY` is also covered by
the same explicit `SELECT_HISTORY` authorization rule as `AS OF LSN` and
`AS OF TIMESTAMP`.

## Practical workflow

```sql
SELECT current_lsn();
SELECT row_lsn('app.users', '1');
SELECT * FROM app.users FOR HISTORY WHERE id = 1;
SELECT * FROM app.users AS OF LSN 4 WHERE id = 1;
```

That sequence is usually enough to explain what changed and when.

## Current view + historical explanation patterns

The most useful temporal workflow is usually not “show me old data” in isolation.
It is:

1. inspect the current visible state,
2. find the relevant temporal token,
3. inspect the mutation trail,
4. reconstruct the historical snapshot that explains the current row or entity.

### Pattern A: row-level explanation

Use this when you want to explain one row's current state.

```sql
SELECT id, status FROM billing.invoices WHERE id = 'inv-1';
SELECT row_lsn('billing.invoices', 'inv-1');
SELECT * FROM billing.invoices FOR HISTORY WHERE id = 'inv-1';
SELECT id, status, total_cents FROM billing.invoices AS OF LSN 12 WHERE id = 'inv-1';
```

Use this pattern when the application thinks mostly in rows and primary keys.

### Pattern B: entity-level explanation

Use this when the application thinks in aggregates rather than isolated rows.

```sql
SELECT id, status FROM billing.invoices WHERE id = 'inv-1';
SELECT entity_version('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_head_lsn('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_version_lsn('billing', 'invoice_aggregate', 'inv-1', 2);
SELECT id, status, total_cents FROM billing.invoices AS OF LSN 12 WHERE id = 'inv-1';
```

Use this when the row is the root of a richer entity lifecycle and version semantics are clearer than raw `LSN`s.

### Pattern C: multi-table snapshot reconstruction

Use this when the explanation spans a root row plus related rows that must be viewed at the same historical point.

```sql
SELECT entity_head_lsn('billing', 'invoice_aggregate', 'inv-1');

SELECT id, invoice_number, total_cents
FROM billing.invoices AS OF LSN 12
WHERE id = 'inv-1';

SELECT id, invoice_id, description, amount_cents
FROM billing.invoice_items AS OF LSN 12
WHERE invoice_id = 'inv-1'
ORDER BY id ASC;
```

This is the normal pattern for reconstructing one replay-safe business snapshot from multiple tables.

### Pattern D: reference explanation

Use this when a versioned reference is involved and you need to know what the engine would capture now.

```sql
SELECT resolve_reference('billing.invoices', 'inv-1');
SELECT row_lsn('billing.invoices', 'inv-1');
SELECT entity_version('billing', 'invoice_aggregate', 'inv-1');
```

If the table is an entity root, `resolve_reference(...)` follows the entity/version surface.
If the table is not an entity root, it follows row-head `LSN` semantics.

If your team is still deciding which meaning it wants a versioned reference to carry, continue with [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md) before locking the schema shape.

## Recommended workflow for debugging

1. find the affected row,
2. inspect `row_lsn(...)` or `entity_version(...)`,
3. inspect `FOR HISTORY`,
4. run a targeted `AS OF LSN` query,
5. compare historical state with current state in Studio.

If the explanation spans multiple tables, repeat step 4 across every table that belongs in the reconstructed snapshot.

## Studio support

ASQL Studio now exposes:

- row mutation history,
- entity history,
- temporal metadata on row detail,
- the Time Explorer for snapshot, diff, and history workflows.

Use Studio when you want visual diffing or exploratory inspection.
Use SQL when you want a replay-safe fact you can automate in tests or incident notes.

## Next step

Continue with [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md).
