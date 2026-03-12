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

## Helpful queries

```sql
SELECT entity_version('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_head_lsn('billing', 'invoice_aggregate', 'inv-1');
SELECT entity_version_lsn('billing', 'invoice_aggregate', 'inv-1', 3);
SELECT resolve_reference('billing.invoices', 'inv-1');
```

## Next step

Continue with [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md).
