# Pharma Manufacturing App on ASQL

Reference application for exploring ASQL adoption in a pharma manufacturing setting with strong traceability and compliance evidence.

This app does not try to turn ASQL into a vertical product.
It should be read as a practical extension of [docs/getting-started/README.md](../docs/getting-started/README.md), especially after chapters 04–09.

The goal is to force a deep adoption path over core ASQL primitives in a case where traceability matters a lot:

- explicit domains,
- `DOMAIN` and `CROSS DOMAIN` transactions,
- entities and versioning,
- `VERSIONED FOREIGN KEY`,
- `AS OF LSN` queries,
- `FOR HISTORY`,
- temporal helpers such as `current_lsn()`, `row_lsn(...)`, `entity_version(...)`, `entity_version_lsn(...)`, and `resolve_reference(...)`,
- deterministic fixtures,
- Go integration via pgwire with `pgx`.

In addition, the sample explicitly models an app-owned case where the team needs to represent:

- compliance controls inspired by 21 CFR Part 11, ALCOA+, and eBR review,
- ISA-88 hierarchy (`master recipe -> unit procedure -> operation -> phase`),
- ISA-95 hierarchy (`site -> area -> process cell -> unit -> equipment`).

## Responsibility boundary

- **Engine-owned concern**: explicit boundaries, versioned references, history, replay-safe snapshots, deterministic fixtures, temporal observability.
- **App-owned concern**: regulatory meaning of signatures, deviation classification, QA review semantics, GxP policies, ISA-88/ISA-95 modeling, and compliance vocabulary.
- **Recommended integration pattern**: use the sample to learn how to compose ASQL primitives, not to push pharmaceutical semantics into the engine.

## What it includes

- [main.go](main.go): Go executable that creates the schema, loads the scenario, and runs temporal inspection.
- [scenario.go](scenario.go): deterministic scenario definition.
- [tx_helpers.go](tx_helpers.go): Go helper pattern for `DOMAIN` and `CROSS DOMAIN` without hiding the transactional boundary.
- [fixtures/pharma-manufacturing-demo-v1.json](fixtures/pharma-manufacturing-demo-v1.json): reproducible fixture for validation and loading with `asqlctl`.
- [FRICTION_LOG.md](FRICTION_LOG.md): document describing the technology frictions encountered while adopting ASQL.

## How to use this app in the onboarding flow

Recommended order:

1. read [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md),
2. read [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md),
3. read [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md),
4. read [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md),
5. read [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md),
6. use this app as a deeper example that combines everything above.

## Domains used

- `recipe`: master recipes, operations, and process parameters.
- `operations`: ISA-95 physical model of site, area, process cell, unit, and equipment.
- `inventory`: material lots and reservations.
- `execution`: process orders, batch steps, and consumed materials.
- `quality`: deviations and QA reviews.
- `compliance`: signatures, attestations, and eBR reviews associated with versioned snapshots.

## Flow exercised

1. create a versioned master recipe with ISA-88 hierarchy in `recipe`,
2. load the ISA-95 physical model in `operations`,
3. load released lots in `inventory`,
4. create a batch order that captures the exact recipe version, the visible `LSN` of the ISA-95 unit, and the reserved lots,
5. start the batch with a phase signature and open the eBR in `compliance`,
6. open a deviation linked to a phase record and place the batch on hold,
7. revise the recipe later to demonstrate the separation between the version captured by the batch and the current version,
8. close the deviation, perform final eBR review, and release the batch with new attestations.

## Local startup

### 1. Start ASQL

From the repository root:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql-pharmaapp
```

### 2. Run the application

In another terminal:

```bash
go run ./pharmaapp -pgwire 127.0.0.1:5433 -mode all
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
go run ./cmd/asqlctl -command fixture-validate -fixture-file pharmaapp/fixtures/pharma-manufacturing-demo-v1.json
```

Load the fixture:

```bash
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file pharmaapp/fixtures/pharma-manufacturing-demo-v1.json
```

## What to observe

After running `-mode all`, pay attention to:

- the need to declare domains before each unit of work,
- the temporal capture columns (`recipe_version`, `lot_version`, `batch_version`, `deviation_version`),
- the difference between entity-version capture (`recipe_version`, `batch_version`, `lot_version`) and row-head `LSN` capture in ISA-95 (`unit_lsn`, `equipment_lsn`),
- the fact that `manufacturing_model_entity` is used only for the top site hierarchy (`site -> area -> process cell`), while `unit` and `equipment` are referenced as direct rows so ASQL does not force the wrong aggregate semantics,
- the separation between the current ISA-88 recipe and the recipe/version actually captured by the batch,
- the relationship between phase execution (`batch_phase_records`) and physical equipment (`operations.equipment_assets`),
- the difference between the current recipe and the recipe captured by the batch,
- the use of `FOR HISTORY` to explain batch, recipe, and deviation states,
- how `resolve_reference(...)` returns the current temporal token for a row or entity,
- how the app still needs its own helpers to turn temporal primitives into business explanations and compliance evidence.

## Useful manual queries

```sql
SELECT current_lsn();
SELECT row_lsn('execution.batch_orders', 'batch-001');
SELECT entity_version('execution', 'batch_record_entity', 'batch-001');
SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 3);
SELECT entity_version('recipe', 'master_recipe_entity', 'recipe-001');
SELECT entity_version('operations', 'manufacturing_model_entity', 'site-001');
SELECT resolve_reference('recipe.master_recipes', 'recipe-001');
SELECT resolve_reference('operations.units', 'unit-001');
SELECT * FROM execution.batch_orders FOR HISTORY WHERE id = 'batch-001';
SELECT * FROM execution.batch_phase_records FOR HISTORY WHERE id = 'phase-rec-002';
SELECT * FROM recipe.master_recipes AS OF LSN 8 WHERE id = 'recipe-001';
```

## Recommended reading

- [docs/getting-started/04-domains-and-transactions.md](../docs/getting-started/04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](../docs/getting-started/05-time-travel-and-history.md)
- [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md)
- [docs/getting-started/07-fixtures-and-seeding.md](../docs/getting-started/07-fixtures-and-seeding.md)
- [docs/getting-started/09-go-sdk-and-integration.md](../docs/getting-started/09-go-sdk-and-integration.md)

If the team is unsure about `ROOT` and `INCLUDES`, review the checklist in [docs/getting-started/06-entities-and-versioned-references.md](../docs/getting-started/06-entities-and-versioned-references.md).
