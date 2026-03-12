# 04. Domains and Transactions

This is the core mental model for adopting ASQL correctly.

## Domains are explicit boundaries

A domain is an isolated schema and constraint boundary inside the engine.

Examples:

- `billing`
- `patients`
- `inventory`
- `manufacturing`

The point is not naming. The point is making boundaries visible.

## Common expectation mismatch

Teams new to ASQL often assume domains are just namespacing.
That usually leads to either:

- one giant domain that hides real business boundaries, or
- too many tiny domains that force unnecessary cross-domain work.

Treat a domain as a boundary for invariants, ownership, and replay reasoning.

## Single-domain transactions

Use these when all work belongs to one boundary.

```sql
BEGIN DOMAIN billing;
INSERT INTO invoices (id, total_cents) VALUES ('inv-1', 12000);
COMMIT;
```

## Cross-domain transactions

Use these only when the write really spans multiple domains.

```sql
BEGIN CROSS DOMAIN billing, inventory;
INSERT INTO invoices (id, total_cents) VALUES ('inv-2', 22000);
UPDATE stock SET quantity = quantity - 1 WHERE sku = 'sku-1';
COMMIT;
```

## Recommended modeling rule

Prefer single-domain transactions by default.
Use cross-domain scope only when the business invariant truly requires atomic work across domains.

## When not to use cross-domain scope

Do not reach for `BEGIN CROSS DOMAIN ...` just because:

- two tables are often queried together,
- the UI shows both concepts on one screen,
- or one workflow step happens immediately after another.

Those are usually application orchestration concerns, not engine-level atomicity requirements.

## Why this matters

The explicit scope is part of ASQL's determinism and operability story.
It reduces ambiguity around:

- what state can change together,
- what replay should reconstruct,
- what auditing must explain.

## Engine-owned vs app-owned concerns

ASQL owns:

- explicit transactional scope,
- deterministic commit order,
- replay-safe state reconstruction,
- historical inspection.

The application still owns:

- workflow stages,
- approval semantics,
- actor meaning,
- compliance vocabulary,
- business-specific policies.

## Rollback behavior

Rollback is explicit and normal.
Use it in development and tests freely.

```sql
BEGIN DOMAIN app;
INSERT INTO users (id, email) VALUES (2, 'temp@example.com');
ROLLBACK;
```

## Querying in a domain context

For unqualified table names, Studio and `asqlctl` generally establish domain context first.
For explicit SQL examples, you can still use qualified names when that is clearer.

## Adoption tip

If domain design is still debated, start with the smallest boundary split that the team can explain clearly.
Refining from one clear boundary to two is easier than unwinding an over-modeled schema map.

## Next step

Continue with [05-time-travel-and-history.md](05-time-travel-and-history.md).
