# 09a. General-Purpose Starter Pack

This starter pack is the recommended baseline for real application teams adopting ASQL.

It is intentionally small.
The goal is not to prescribe one product architecture.
The goal is to give teams a repeatable starting point for application code that sits on top of ASQL.

Use this after:

- explicit domain boundaries are named,
- the first fixture is validated,
- and the first pgwire integration path is clear.

## What belongs in the starter pack

Keep the starter pack limited to reusable conventions that help many application types:

- identifier conventions,
- timestamp conventions,
- audit metadata shape,
- transaction helper patterns,
- temporal read helper patterns,
- fixture-first workflow conventions.

Do not use the starter pack to smuggle business workflow semantics into ASQL.

## 1. Identifier conventions

Recommended baseline:

- use explicit stable IDs in fixtures and tests,
- use application-generated IDs instead of hidden database generation,
- choose one format per aggregate family and keep it consistent.

Examples:

- `cust-001`
- `acct-001`
- `invoice-001`
- `transfer-001`

Why this helps:

- fixtures stay reviewable,
- replay/debugging notes stay readable,
- test scenarios are easier to compare across runs.

## 2. Timestamp conventions

Recommended baseline:

- timestamps used by the application should be explicit and app-owned,
- timestamps in fixtures should be literal values, not runtime-generated expressions,
- use ASQL temporal helpers for engine history, not as a substitute for business timestamps.

Rule of thumb:

- use app-owned timestamps for business meaning,
- use `LSN`/history/entity helpers for replay-safe explanation and debugging.

## 3. Audit metadata shape

Recommended generic app-owned fields:

- `actor_id`
- `reason`
- `artifact_type`
- `artifact_id`
- `occurred_at`
- `payload_json`

Why this stays app-owned:

- the engine should help with deterministic history,
- the application should still decide what the action means and which metadata matters.

## 4. Transaction helper conventions

Recommended baseline:

- keep `DOMAIN` vs `CROSS DOMAIN` decisions explicit,
- wrap begin/commit/rollback boilerplate in small helpers,
- do not hide boundary decisions inside generic repositories.

Reference pattern:

- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [../../bankapp/tx_helpers.go](../../bankapp/tx_helpers.go)

## 5. Temporal read helper conventions

Recommended baseline:

- use `current_lsn()`, `row_lsn(...)`, `entity_version(...)`, `entity_version_lsn(...)`, and `FOR HISTORY` directly in explicit helper functions,
- keep “current view + historical explanation” as one named application pattern,
- avoid inventing a separate temporal API layer too early.

Reference pattern:

- [05-time-travel-and-history.md](05-time-travel-and-history.md)
- [../reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)

## 6. Fixture-first workflow conventions

Recommended baseline:

1. create one small fixture per important workflow,
2. validate it before wiring handlers,
3. use it in integration tests,
4. use the same fixture as demo/debug seed data when practical.

This makes schema, ordering, and boundary mistakes visible early.

## 7. Recommended repository shape

At minimum, most application teams should have:

- one place for transaction helpers,
- one place for temporal read helpers,
- one place for fixture files,
- one place for app-owned audit metadata conventions.

The exact package structure is application-specific.
The important part is that these concerns are explicit and reusable.

## Responsibility boundary reminder

- **Engine-owned**: history, replay, domains, entity/version primitives, deterministic fixtures.
- **App-owned**: workflow meaning, audit semantics, actor meaning, product-specific timestamps and policies.
- **Recommended integration pattern**: start with a small reusable starter pack in app code rather than expecting the engine to absorb those choices.

## BankApp as an extension, not a parallel track

Use [../../bankapp/README.md](../../bankapp/README.md) as a deeper extension of chapters 04–09 in getting-started.

Do not treat it as a separate onboarding path.
Treat it as the place where the starter-pack conventions are exercised in one realistic multi-domain example.

## Next step

Return to [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md) or continue with [10-adoption-playbook.md](10-adoption-playbook.md).