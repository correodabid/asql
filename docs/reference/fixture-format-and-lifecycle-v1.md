# ASQL Fixture Format and Lifecycle v1

## Purpose

Define the first stable fixture contract for deterministic demo, test, and benchmark scenarios.

## Scope decision

ASQL fixture files are **whole-scenario** documents in v1.

A fixture file contains an ordered list of transaction steps. Each step declares:

- transaction scope: `domain` or `cross`
- participating domains
- ordered SQL statements executed inside that transaction

This keeps the public contract small while still supporting:

- single-domain seeds
- cross-domain scenarios
- realistic aggregate/versioned-reference flows
- deterministic replay-oriented demos and benchmarks

Entity-level or domain-level export/import can be added later without changing the v1 scenario shape.

## File format

Fixtures are stored as strict JSON documents.

```json
{
  "version": "v1",
  "name": "healthcare-billing-demo",
  "description": "Small multi-domain demo.",
  "steps": [
    {
      "name": "patients schema",
      "mode": "domain",
      "domains": ["patients"],
      "statements": [
        "CREATE TABLE patients.patients (id TEXT PRIMARY KEY, full_name TEXT)"
      ]
    },
    {
      "name": "invoice seed",
      "mode": "cross",
      "domains": ["billing", "patients"],
      "statements": [
        "INSERT INTO patients.patients (id, full_name) VALUES ('patient-1', 'Ana Lopez')",
        "INSERT INTO billing.invoices (id, patient_id, total_cents) VALUES ('invoice-1', 'patient-1', 125000)"
      ]
    }
  ]
}
```

## Lifecycle semantics

Each step is executed as:

1. `BEGIN DOMAIN <name>` or `BEGIN CROSS DOMAIN <a>, <b>, ...`
2. execute all listed statements in order
3. `COMMIT`

The fixture loader owns transaction boundaries. Therefore fixture statements must not contain:

- `BEGIN ...`
- `START TRANSACTION`
- `COMMIT`
- `ROLLBACK`

## Determinism rules

Fixtures are intended to be replay-stable and reviewable.

v1 requires explicit business data inside statements:

- explicit IDs instead of generated UUIDs
- explicit timestamps instead of runtime clocks
- explicit ordering through step and statement order

The validation workflow rejects statements containing known non-deterministic tokens such as:

- `NOW()`
- `CURRENT_TIMESTAMP`
- `RANDOM()`
- `UUID()`
- `UUID_V7()`

## Validation workflow

Validation happens in two layers:

1. **spec validation**
   - strict JSON decoding
   - supported `version`
   - valid `mode`
   - non-empty domain list
   - non-empty statements
   - no duplicate domains in a step
   - no transaction-control statements inside steps
   - no non-deterministic tokens

2. **dry-run validation**
   - load fixture into a fresh ephemeral ASQL engine
   - execute every step in order
   - fail before touching a live server if schema ordering, SQL validity, FK checks, or versioned-reference resolution is broken

This makes fixture validation a real workflow rather than a schema-only lint.

## Interpreting validation failures during adoption

Validation errors should usually be read as one of four feedback types:

- **determinism problem** — implicit timestamps, random values, generated identifiers,
- **transaction-boundary problem** — fixture statements still carry manual transaction control,
- **ordering or dependency problem** — schema, parent rows, or domains appear too late,
- **modeling problem** — entity boundaries or versioned-reference expectations are wrong.

The recommended first response is not to weaken fixture strictness.
The recommended first response is to clarify the scenario and make the workflow self-contained.

## Import workflow

`asqlctl` exposes three fixture commands:

- `fixture-validate`
- `fixture-load`
- `fixture-export`

`fixture-load` always runs validation first, then applies the fixture to the target pgwire endpoint.

`fixture-export` reads schema metadata from the pgwire admin surface, extracts current rows from the selected domains, and writes a deterministic v1 fixture file. The minimal export path is intentionally strict:

- at least one domain must be selected
- exported tables must have a primary key so row order is replay-stable
- cross-domain dependencies must be included in the selected domain set
- dependency cycles fail export instead of producing unstable output

## Current limitations

v1 does **not** yet provide:

- partial entity/domain extraction beyond explicit domain selection
- fixture variables/templates
- automatic idempotent upsert semantics

Those remain follow-on work.

## Reference fixture pack

The initial reference pack is:

- [path/to/your-fixture.json](../path/to/your-fixture.json)

It demonstrates:

- multi-domain schema creation
- entity definitions
- versioned foreign key auto-capture
- historical entity evolution across multiple commits

## Minimal export example

```bash
go run ./cmd/asqlctl -command fixture-export \
  -pgwire 127.0.0.1:5433 \
  -domains billing,patients,clinical \
  -fixture-file path/to/your-export.json
```

The exported file can then be checked with `fixture-validate` or reapplied with `fixture-load`.
