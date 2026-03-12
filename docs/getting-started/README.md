# ASQL Getting Started

This guide set is the recommended entry point for developers adopting ASQL in a real application.

It is organized by topic so teams can move from first run to production-oriented workflows without reverse engineering engine concepts.

## Internalize these expectations early

- ASQL is not just "PostgreSQL with a few extras". The pgwire surface is the main application runtime, but the product model adds explicit domains, replay-safe history, and deterministic fixtures.
- Domains are application-visible boundaries, not cosmetic schema prefixes.
- Time-travel and `FOR HISTORY` are normal debugging workflows, not rare incident-only tools.
- Most business workflow semantics still belong in the application. ASQL should make those workflows easier to model and inspect, not absorb them.

If your team is new to this distinction, read [00-engine-owned-vs-app-owned.md](00-engine-owned-vs-app-owned.md) before the main walkthrough.

## Recommended reading order

1. [00-engine-owned-vs-app-owned.md](00-engine-owned-vs-app-owned.md) — responsibility boundaries before adoption
2. [01-overview.md](01-overview.md) — what ASQL is and when it fits
3. [02-install-and-run.md](02-install-and-run.md) — local setup and first server
4. [03-first-database.md](03-first-database.md) — first schema, write, and read
5. [04-domains-and-transactions.md](04-domains-and-transactions.md) — the core mental model
6. [05-time-travel-and-history.md](05-time-travel-and-history.md) — replay-safe reads and debugging
7. [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md) — aggregate-oriented workflows
8. [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md) — deterministic demo and test data
9. [08-studio-cli-and-daily-workflow.md](08-studio-cli-and-daily-workflow.md) — daily developer tooling
10. [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md) — integrating ASQL into Go services
11. [09a-general-purpose-starter-pack.md](09a-general-purpose-starter-pack.md) — app-side starter conventions on top of ASQL
12. [10-adoption-playbook.md](10-adoption-playbook.md) — how to roll ASQL into an existing app
13. [11-troubleshooting.md](11-troubleshooting.md) — common problems and fixes

## Fast paths

- Want the shortest possible path? Start with [02-install-and-run.md](02-install-and-run.md) and [03-first-database.md](03-first-database.md).
- Migrating from SQLite-style workloads? Read [10-adoption-playbook.md](10-adoption-playbook.md).
- Want reproducible demo/test data? Jump to [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md).
- Want to understand ASQL's differentiators before coding? Start with [01-overview.md](01-overview.md).
- Want one deeper example app that forces domains, entities, history, temporal helpers, and fixtures into the same workflow? Study [../../bankapp/README.md](../../bankapp/README.md) after chapters 04–09.

## Fixture-first onboarding path

For real application adoption, the recommended fast path is often fixture-first:

1. read [04-domains-and-transactions.md](04-domains-and-transactions.md),
2. read [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md) if aggregates matter,
3. validate and load one deterministic fixture with [07-fixtures-and-seeding.md](07-fixtures-and-seeding.md),
4. only then move into service integration in [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md),
5. use [../../bankapp/README.md](../../bankapp/README.md) as the deeper multi-domain example.

This usually exposes modeling and transaction-scope mistakes earlier than jumping straight into handlers or UI code.

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
- [00-engine-owned-vs-app-owned.md](00-engine-owned-vs-app-owned.md)
- [09a-general-purpose-starter-pack.md](09a-general-purpose-starter-pack.md)
- [10-adoption-playbook.md](10-adoption-playbook.md)
- [../../bankapp/README.md](../../bankapp/README.md)
- [../../bankapp/FRICTION_LOG.md](../../bankapp/FRICTION_LOG.md)
