# 06. Entities and Versioned References

This topic explains the aggregate-oriented layer on top of ASQL's row-and-WAL core.

## What an entity is

An entity is ASQL's aggregate boundary.

Example:

```sql
CREATE ENTITY recipe_aggregate (
  ROOT recipes,
  INCLUDES recipe_steps, recipe_checks
)
```

Rules:

- the root table defines aggregate identity,
- included tables participate in the same lifecycle,
- committed mutations advance the entity version deterministically.

## Why entities matter

Without entities, applications often end up carrying raw row IDs plus raw `LSN`s.
ASQL still allows that, but entities make many workflows easier to reason about.

## Versioned foreign keys

A versioned foreign key captures a temporal token for the referenced row or entity snapshot.

Conceptually:

- if you reference a normal table, ASQL captures row-head `LSN`,
- if you reference an entity root, ASQL captures entity version.

## Row-head `LSN` capture vs entity-version capture

:::warning[One of the most important adoption choices]
Two schemas can both use `VERSIONED FOREIGN KEY`, but mean very different things:

- **Row-head `LSN` capture** says: _"I need the latest visible mutation point of this row."_
- **Entity-version capture** says: _"I need the business-facing version of this aggregate."_

If teams do not make this choice explicitly, they usually end up with confusing temporal columns and unclear historical expectations.
:::

:::tabs
:::tab[Option A — Row-based reference]
Use when the referenced table is mostly standalone and row history is already the clearest model.

```sql
CREATE TABLE clinical.admissions (
  id TEXT PRIMARY KEY,
  patient_id TEXT NOT NULL,
  status TEXT NOT NULL,
  patient_lsn INT,
  VERSIONED FOREIGN KEY (patient_id)
    REFERENCES patients.patients(id)
    AS OF patient_lsn
)
```

**Semantics**

- Temporal token follows the current visible **row-head `LSN`** of `patients.patients`.
- Downstream code reasons about one row snapshot.
- Historical explanation starts from `row_lsn(...)` and `FOR HISTORY`.

**Use when**

- The referenced row is mostly independent.
- No richer aggregate lifecycle needs to be preserved.
- Capturing a row mutation point is clearer than introducing an entity.
:::tab[Option B — Entity-based reference]
Use when the referenced table is really the root of a richer aggregate and downstream data should capture the aggregate version rather than one row mutation point.

```sql
CREATE TABLE recipe.master_recipes (
  id TEXT PRIMARY KEY,
  recipe_code TEXT UNIQUE,
  title TEXT NOT NULL,
  status TEXT NOT NULL
);

CREATE TABLE recipe.recipe_operations (
  id TEXT PRIMARY KEY,
  recipe_id TEXT NOT NULL REFERENCES master_recipes(id),
  operation_code TEXT NOT NULL,
  instruction_text TEXT NOT NULL
);

CREATE ENTITY master_recipe_entity (
  ROOT master_recipes,
  INCLUDES recipe_operations
);

CREATE TABLE execution.batch_orders (
  id TEXT PRIMARY KEY,
  recipe_id TEXT NOT NULL,
  recipe_version INT,
  status TEXT NOT NULL,
  VERSIONED FOREIGN KEY (recipe_id)
    REFERENCES recipe.master_recipes(id)
    AS OF recipe_version
)
```

**Semantics**

- Temporal token follows the **entity version** of `master_recipe_entity`.
- Downstream code preserves a replay-safe aggregate snapshot.
- Historical explanation starts from `entity_version(...)`, `entity_head_lsn(...)`, and `entity_version_lsn(...)`.

**Use when**

- The application already thinks in one aggregate root.
- Related child tables belong to the same lifecycle.
- Downstream references should capture a business revision, not just a row mutation point.
:::

## Practical decision rule

When deciding between row-head `LSN` capture and entity-version capture, ask:

1. Is the referenced thing really one standalone row, or a multi-table aggregate?
2. Will downstream code explain history in row terms or aggregate-version terms?
3. If child rows change, should downstream references still mean “the same business version”?

Use **row-head `LSN` capture** when the answer is mostly row-centric.
Use **entity-version capture** when the answer is mostly aggregate-centric.

## Common mistake to avoid

:::danger[Do not create entities to make references "look structured"]
That usually creates a worse model:

- Temporal columns look business-meaningful, but the aggregate boundary is still fuzzy.
- `resolve_reference(...)` returns entity semantics that the team cannot explain.
- Debugging gets harder because row history and aggregate history are now mixed without a clear reason.

Likewise, do not keep everything row-based if the application already reasons in aggregate revisions. That usually pushes too much temporal interpretation into application code.
:::

## Automatic capture

In the normal path you do not need to manually supply the temporal token.
The engine resolves it from the transaction-visible snapshot.

This includes same-transaction visibility.
If statement 1 creates or updates a referenced entity, statement 2 in the same transaction can capture that pending visible version.

## When to use explicit override

Use explicit `AS OF` values only when you need exact historical control, for example:

- repair scripts,
- migration workflows,
- precise backfill logic.

## Recommended adoption rule

For business workflows:

- prefer entity versions over raw `LSN`s,
- keep raw `LSN`s for debugging and advanced repair work,
- model aggregate boundaries explicitly.

## Entity modeling checklist

Use this checklist before adding `CREATE ENTITY`.

:::details[1. Should this be an entity at all?]
**Usually yes if**

- The application already thinks in one stable aggregate root.
- Several tables participate in one lifecycle.
- Historical explanation is easier at aggregate level than row level.
- Versioned references should follow business aggregate semantics instead of raw row-head `LSN`s.

**Usually no if**

- The table is mostly standalone.
- The boundary exists only because rows are often queried together.
- The team still cannot explain the aggregate lifecycle.
- Raw row history is already the clearest model.
:::

:::details[2. Is the `ROOT` table correct?]
Your `ROOT` table should usually be the table that defines:

- Aggregate identity.
- The primary business key for the aggregate.
- The state transition developers talk about first.

**Good signs**

- One row clearly anchors the aggregate.
- Related rows make sense as children of that root.
- Other tables rarely need to exist independently of the root lifecycle.

**Warning signs**

- The chosen root is just the most convenient join point.
- Two tables could both plausibly be the root.
- Identity really lives somewhere else.
:::

:::details[3. Which tables belong in `INCLUDES`?]
**Include tables when they**

- Participate in the same lifecycle as the root.
- Should move version history together with the root.
- Are part of the replay-safe explanation of one aggregate state.

**Do not include tables just because**

- They are frequently joined.
- They appear on the same screen.
- They reference the root without sharing the same lifecycle.
:::

:::details[4. Will versioned references be clearer with entity semantics?]
Prefer entity semantics when downstream references should capture:

- The business version of the aggregate.
- Not the latest visible row-head `LSN` of one table.

If downstream code only needs the latest row mutation point, a plain row-based reference may be enough.
:::

:::details[5. Can the team explain the lifecycle in one sentence?]
Before creating the entity, make sure the team can say something like:

- _"An invoice is rooted in `invoices` and includes `invoice_items` because both belong to one aggregate lifecycle."_
- Or _"A recipe is rooted in `recipes` and includes steps/checks because versioned references should follow recipe revisions, not individual row heads."_

If that sentence is still fuzzy, **delay the entity and use rows first**.
:::

## Quick anti-pattern list

:::warning[Avoid these common mistakes]
- Creating an entity for every table by default.
- Choosing `ROOT` based on convenience rather than identity.
- Adding `INCLUDES` tables that are only query-adjacent.
- Using entities to hide unclear application modeling.
- Forcing entity semantics where row history is already enough.
:::

## Example mental model

- `billing.invoices` + `billing.invoice_items` is often a good entity boundary.
- `identity.customers` + `identity.customer_contacts` can be a good entity if contacts belong to the same customer lifecycle.
- a reporting table that is rebuilt from other sources is usually not a good entity root.

## Helpful queries

```sql
SELECT row_lsn('patients.patients', 'patient-1');
SELECT entity_version('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_head_lsn('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_version_lsn('billing', 'invoice_aggregate', 'inv-1', 3);
SELECT resolve_reference('patients.patients', 'patient-1');
SELECT resolve_reference('billing.invoices', 'inv-1');
```

## Next step

Continue with [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md).
