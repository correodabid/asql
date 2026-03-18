# BankApp on ASQL

Reference application for exploring ASQL adoption in a banking management system.

This app is not a parallel onboarding path.
It should be read as a practical extension of [docs/getting-started/README.md](../docs/getting-started/README.md), especially after chapters 04–09.

The goal is not to model a complete banking product, but to force a deep adoption path over core ASQL capabilities:

- explicit domains,
- `DOMAIN` and `CROSS DOMAIN` transactions,
- entities and versioning,
- `VERSIONED FOREIGN KEY`,
- `AS OF LSN` queries,
- `FOR HISTORY`,
- temporal helpers such as `current_lsn()`, `row_lsn(...)`, `entity_version(...)`, and `resolve_reference(...)`,
- deterministic fixtures.

## Responsibility boundary

- **Engine-owned concern**: explicit domains, versioned references, history, `AS OF LSN` snapshots, deterministic fixtures.
- **App-owned concern**: risk rules, approval semantics, regulatory vocabulary, event meaning, and business policies.
- **Recommended integration pattern**: use the sample to learn how to compose ASQL primitives, not to turn ASQL into a vertical banking product.

## What it includes

- [main.go](main.go): Go executable that creates the schema, loads the scenario, and runs temporal inspection.
- [scenario.go](scenario.go): deterministic scenario definition.
- [tx_helpers.go](tx_helpers.go): Go helper pattern for `DOMAIN` and `CROSS DOMAIN` without hiding the transactional boundary.
- [fixtures/banking-core-demo-v1.json](fixtures/banking-core-demo-v1.json): reproducible fixture for validation and loading with `asqlctl`.
- [FRICTION_LOG.md](FRICTION_LOG.md): document describing the technology frictions encountered while adopting ASQL.

## How to use this app in the onboarding flow

Recommended order:

1. read [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md),
2. read [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md),
3. read [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md),
4. read [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md),
5. review the compact adoption conventions in [docs/getting-started/10-adoption-playbook.md](../docs/getting-started/10-adoption-playbook.md),
6. use this app as a deeper example that combines everything above.

## Domains used

- `identity`: customers and contacts.
- `ledger`: accounts and ledger entries.
- `payments`: transfer requests and events.
- `risk`: risk reviews.

## Flow exercised

1. register two customers in `identity`,
2. open accounts in `ledger`,
3. create a transfer in `payments` with versioned references to the customer and accounts,
4. approve it in `risk`,
5. settle it in `payments` + `ledger`,
6. update the customer afterwards to demonstrate separation between the captured snapshot and the current state.

## Local startup

### 1. Start ASQL

From the repository root:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql-bankapp
```

### 2. Run the application

In another terminal:

```bash
go run ./bankapp -pgwire 127.0.0.1:5433 -mode all
```

`-mode all` does three things:

- applies the schema,
- executes the scenario,
- prints current and historical reads.

You can also use:

- `-mode schema`
- `-mode scenario`
- `-mode inspect`
- `-mode print-sql`

Important: `schema` and `all` are intended for a fresh `data-dir`.

## Fixture-first flow

Validate the fixture:

```bash
go run ./cmd/asqlctl -command fixture-validate -fixture-file bankapp/fixtures/banking-core-demo-v1.json
```

Load the fixture:

```bash
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file bankapp/fixtures/banking-core-demo-v1.json
```

## What to observe

After running `-mode all`, pay attention to:

- the need to declare domains before each unit of work,
- the temporal capture columns (`customer_version`, `source_account_version`, `destination_account_version`, `transfer_version`),
- the difference between the current state and the `AS OF LSN` state,
- the use of `FOR HISTORY` to explain a transition,
- how `resolve_reference(...)` returns the current temporal token for a row or entity.

## Useful manual queries

```sql
SELECT current_lsn();
SELECT row_lsn('payments.transfer_requests', 'tr-001');
SELECT entity_version('payments', 'transfer_entity', 'tr-001');
SELECT entity_head_lsn('payments', 'transfer_entity', 'tr-001');
SELECT entity_version_lsn('payments', 'transfer_entity', 'tr-001', 1);
SELECT resolve_reference('identity.customers', 'cust-001');
SELECT * FROM payments.transfer_requests FOR HISTORY WHERE id = 'tr-001';
```

## Recommended reading

- [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md)
- [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md)
- [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md)
- [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md)

If the team is unsure about `ROOT` and `INCLUDES`, review the checklist in [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md).
