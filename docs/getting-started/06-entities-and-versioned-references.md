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

### 1. Should this be an entity at all?

Usually yes if:

- the application already thinks in one stable aggregate root,
- several tables participate in one lifecycle,
- historical explanation is easier at aggregate level than row level,
- versioned references should follow business aggregate semantics instead of raw row-head `LSN`s.

Usually no if:

- the table is mostly standalone,
- the boundary exists only because rows are often queried together,
- the team still cannot explain the aggregate lifecycle,
- raw row history is already the clearest model.

### 2. Is the `ROOT` table correct?

Your `ROOT` table should usually be the table that defines:

- aggregate identity,
- the primary business key for the aggregate,
- the state transition developers talk about first.

Good signs:

- one row clearly anchors the aggregate,
- related rows make sense as children of that root,
- other tables rarely need to exist independently of the root lifecycle.

Warning signs:

- the chosen root is just the most convenient join point,
- two tables could both plausibly be the root,
- identity really lives somewhere else.

### 3. Which tables belong in `INCLUDES`?

Include tables when they:

- participate in the same lifecycle as the root,
- should move version history together with the root,
- are part of the replay-safe explanation of one aggregate state.

Do not include tables just because:

- they are frequently joined,
- they appear on the same screen,
- or they reference the root without sharing the same lifecycle.

### 4. Will versioned references be clearer with entity semantics?

Prefer entity semantics when downstream references should capture:

- the business version of the aggregate,
- not the latest visible row-head `LSN` of one table.

If downstream code only needs the latest row mutation point, a plain row-based reference may be enough.

### 5. Can the team explain the lifecycle in one sentence?

Before creating the entity, make sure the team can say something like:

- “an invoice is rooted in `invoices` and includes `invoice_items` because both belong to one aggregate lifecycle”,
- or “a recipe is rooted in `recipes` and includes steps/checks because versioned references should follow recipe revisions, not individual row heads”.

If that sentence is still fuzzy, delay the entity and use rows first.

## Quick anti-pattern list

Avoid these common mistakes:

- creating an entity for every table by default,
- choosing `ROOT` based on convenience rather than identity,
- adding `INCLUDES` tables that are only query-adjacent,
- using entities to hide unclear application modeling,
- forcing entity semantics where row history is already enough.

## Example mental model

- `billing.invoices` + `billing.invoice_items` is often a good entity boundary.
- `identity.customers` + `identity.customer_contacts` can be a good entity if contacts belong to the same customer lifecycle.
- a reporting table that is rebuilt from other sources is usually not a good entity root.

## Helpful queries

```sql
SELECT entity_version('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_head_lsn('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_version_lsn('billing', 'invoice_aggregate', 'inv-1', 3);
SELECT resolve_reference('billing.invoices', 'inv-1');
```

For a larger example that uses multiple entities and versioned references together, see [../../bankapp/README.md](../../bankapp/README.md).

## Next step

Continue with [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md).
