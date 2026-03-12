# Aggregate reference semantics v1

This note defines the first aggregate-oriented application model for ASQL.

## Goal

Keep ASQL's deterministic row-and-WAL core intact while making the public model easier to use for business aggregates such as:

- healthcare episodes and admissions
- manufacturing batches and process orders
- finance invoices and settlement cases

## Core stance

ASQL keeps three layers visible, but they are not equal in day-to-day usage:

1. rows and `LSN`s remain the storage and replay truth
2. entities define aggregate boundaries across related tables
3. aggregate references should prefer entity versions over raw `LSN`s in application-facing workflows

Advanced users can still work directly with `LSN`s. Normal product workflows should not require it.

## Aggregate model

An aggregate in ASQL is represented by an entity:

- one root table defines aggregate identity
- included tables participate in the same lifecycle version
- any committed change to the root or included tables advances the entity version deterministically

This makes the entity version the default business snapshot token.

## Reference semantics

When a table references another aggregate, ASQL should treat that relationship as a reference to the aggregate snapshot visible to the transaction.

### Default capture

For `VERSIONED FOREIGN KEY` references to entity roots:

- the engine captures the visible entity version automatically
- same-transaction earlier writes are visible to later statements
- replay reconstructs the same captured version from WAL order

For non-entity tables:

- the engine captures the visible row-head `LSN`
- raw `LSN` references remain available for low-level temporal control

### Explicit override

Applications may still supply the `AS OF` value directly when they need:

- a historical aggregate version
- a historical row `LSN`
- reproducible migration or repair workflows with explicit temporal tokens

## Business examples

### Healthcare

- `patients.patient_entity` defines the patient aggregate
- `clinical.admissions` can reference a patient entity version when an admission is opened
- later patient profile changes do not silently rewrite the admission's captured patient context

### Manufacturing

- `master.recipe_entity` defines the batch recipe aggregate
- `execution.process_orders` capture the visible recipe version at order release time
- downstream analysis can answer which recipe snapshot governed that production run

### Finance

- `billing.invoice_entity` defines the invoice aggregate
- `collections.payment_plans` can capture the invoice version that was current when the plan was negotiated
- later invoice corrections remain visible historically without mutating prior agreements

## Public API bias

ASQL should bias public workflows toward the following concepts:

- `entity_version(...)` for aggregate snapshot lookup
- `entity_head_lsn(...)` for operational debugging
- `resolve_reference(...)` for showing what auto-capture will record
- fixture workflows that encode business scenarios, not manual `LSN` plumbing

## Design constraints

Aggregate-facing features must preserve the core product principles:

- deterministic replay stays authoritative
- cross-domain side effects remain explicit
- aggregate references never imply hidden cross-domain writes
- entity versions are derived from ordered mutations, not wall-clock time

## Pilot recommendation

The narrowest next vertical slice is an aggregate-friendly read helper that resolves a root ID plus entity version into a deterministic time-travel token.

## Pilot chosen in v1

The first pilot primitive is:

- `entity_version_lsn(domain, entity, root_pk, version)`

It returns the commit `LSN` for a specific entity version. Applications can then compose that token with existing `AS OF LSN` reads to reconstruct the aggregate snapshot for that version.

### Acceptance criteria

- the helper is available through pgwire
- missing versions return SQL `NULL`
- latest version lookup matches `entity_head_lsn(...)`
- historical version lookup returns an older deterministic `LSN`

This keeps the pilot small, uses existing entity/version infrastructure, and improves real application workflows without widening write semantics prematurely.
