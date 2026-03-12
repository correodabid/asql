# ASQL Getting Started

This guide set is the recommended entry point for developers adopting ASQL in a real application.

It is organized by topic so teams can move from first run to production-oriented workflows without reverse engineering engine concepts.

## Internalize these expectations early

- ASQL is not just "PostgreSQL with a few extras". The pgwire surface is the main application runtime, but the product model adds explicit domains, replay-safe history, and deterministic fixtures.
- Domains are application-visible boundaries, not cosmetic schema prefixes.
- Time-travel and `FOR HISTORY` are normal debugging workflows, not rare incident-only tools.
- Most business workflow semantics still belong in the application. ASQL should make those workflows easier to model and inspect, not absorb them.

## Recommended reading order

1. [01-overview.md](01-overview.md) — what ASQL is and when it fits
2. [02-install-and-run.md](02-install-and-run.md) — local setup and first server
3. [03-first-database.md](03-first-database.md) — first schema, write, and read
4. [04-domains-and-transactions.md](04-domains-and-transactions.md) — the core mental model
5. [05-time-travel-and-history.md](05-time-travel-and-history.md) — replay-safe reads and debugging
6. [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md) — aggregate-oriented workflows
7. [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md) — deterministic demo and test data
8. [08-studio-cli-and-daily-workflow.md](08-studio-cli-and-daily-workflow.md) — daily developer tooling
9. [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md) — integrating ASQL into Go services
10. [10-adoption-playbook.md](10-adoption-playbook.md) — how to roll ASQL into an existing app
11. [11-troubleshooting.md](11-troubleshooting.md) — common problems and fixes

## Fast paths

- Want the shortest possible path? Start with [02-install-and-run.md](02-install-and-run.md) and [03-first-database.md](03-first-database.md).
- Migrating from SQLite-style workloads? Read [10-adoption-playbook.md](10-adoption-playbook.md).
- Want reproducible demo/test data? Jump to [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md).
- Want to understand ASQL's differentiators before coding? Start with [01-overview.md](01-overview.md).

## If adoption feels harder than expected

That usually means one of these is still fuzzy:

- the first real domain boundaries,
- whether a workflow truly needs cross-domain atomicity,
- when to use entities and versioned references,
- how much compliance or business semantics should stay in application code,
- or how to convert ad hoc seed SQL into deterministic fixtures.

Those are the main adoption friction points this guide tries to make explicit.

## Related docs

- [10-min.md](10-min.md)
- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [../reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)
- [../reference/temporal-introspection-surface-v1.md](../reference/temporal-introspection-surface-v1.md)
- [../reference/aggregate-reference-semantics-v1.md](../reference/aggregate-reference-semantics-v1.md)
