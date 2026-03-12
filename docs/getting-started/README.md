# ASQL Getting Started

This guide set is the recommended entry point for developers adopting ASQL in a real application.

It is organized by topic so teams can move from first run to production-oriented workflows without reverse engineering engine concepts.

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

## Related docs

- [../getting-started-10-min.md](../getting-started-10-min.md)
- [../cookbook-go-sdk.md](../cookbook-go-sdk.md)
- [../fixture-format-and-lifecycle-v1.md](../fixture-format-and-lifecycle-v1.md)
- [../temporal-introspection-surface-v1.md](../temporal-introspection-surface-v1.md)
- [../aggregate-reference-semantics-v1.md](../aggregate-reference-semantics-v1.md)
