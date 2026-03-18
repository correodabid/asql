# Pharma Manufacturing App – ASQL Friction Log

## Objective

This document captures frictions observed while building a reference application that uses ASQL intensively in a strong-traceability context.

The focus is strictly technological:

- engine friction,
- mental-model friction,
- SQL/pgwire surface friction,
- integration ergonomics,
- and operational workflow friction.

Pharma-domain friction is intentionally out of scope.
These observations are also not meant to argue that ASQL should absorb regulatory semantics or GxP vocabulary.

## What the sample deliberately forced

The sample deliberately uses:

- 5 explicit domains,
- `BEGIN DOMAIN ...` and `BEGIN CROSS DOMAIN ...` transactions,
- entities (`CREATE ENTITY`),
- `VERSIONED FOREIGN KEY`,
- temporal helpers,
- `FOR HISTORY`,
- `AS OF LSN`,
- deterministic fixtures,
- Go integration via pgwire with `pgx`,
- a case where the application needs to explain exactly which recipe, which lots, and which batch version were actually captured.

That makes it visible where a team will struggle during the first weeks of adoption.

## How to read this log

This log does not try to prove that ASQL is a poor fit for the use case.
It tries to identify which adoption costs truly belong to the engine and should therefore be answered with one of these product outputs:

- better primitives,
- better diagnostics,
- better documentation,
- better tooling,
- better SDKs or integration patterns,
- better adoption-oriented observability.

The reading rule is simple:

- if the friction demands more clarity or ergonomics around boundaries, history, determinism, or temporal references, it is probably ASQL friction,
- if the friction demands regulatory vocabulary, sector workflows, or business semantics, it does not belong in the engine.

## Observed frictions

### 1. The transactional boundary stops being an invisible decision

**Where it appears**

The app must decide on every operation whether the work lives in a single domain or should use `CROSS DOMAIN` across `recipe`, `inventory`, `execution`, `quality`, and `compliance`.

**Why this is ASQL friction**

This is a central property of the product: ASQL requires explicit boundaries.
It is not a detail of the sample.

**Adoption impact**

- forces early redesign of the service layer,
- makes visible decisions that many teams currently hide inside repositories or ORMs,
- increases the need for transaction helpers from the very first sprint.

**Product opportunity**

- official patterns for transactional-scope helpers,
- heuristics and metrics for detecting `CROSS DOMAIN` overuse,
- a short guide for discovering the first useful boundaries.

**Priority**: P0

---

### 2. Temporal versioning invades the physical schema from day one

**Where it appears**

Explicit columns such as `recipe_version`, `lot_version`, `batch_version`, and `deviation_version` appear in ordinary working tables.

**Why this is ASQL friction**

Temporal semantics are not just a query-time add-on.
They change how tables, migrations, and references are modeled.

**Adoption impact**

- the team must decide when to capture entity version and when to capture row `LSN`,
- the schema gains technical columns that feel unfamiliar,
- design-review cost increases because future history depends on the current shape.

**Product opportunity**

- schema templates for versioned references,
- linters or suggestions for `VERSIONED FOREIGN KEY`,
- comparative guidance between ordinary references, row-based references, and entity-based references.

**Priority**: P0

---

### 3. `CREATE ENTITY` adds clarity, but demands more mature modeling than many teams have at the start

**Where it appears**

Teams must decide early that `master_recipes`, `material_lots`, `batch_orders`, and `deviations` are valid roots, and which tables belong in `INCLUDES`.

**Why this is ASQL friction**

ASQL offers a powerful aggregate layer, but there is no automatic translation from a conventional relational schema.

**Adoption impact**

- slows down teams that do not yet understand their aggregates,
- can lock in bad entities too early,
- mixes logical design and historical-debugging strategy into the same discussion.

**Product opportunity**

- a more operational checklist for choosing `ROOT` and `INCLUDES`,
- validations for suspiciously large or ambiguous entities,
- modeling examples across industries without pushing engine verticalization.

**Priority**: P1

---

### 4. Real historical explanation is still too manual for the value ASQL promises

**Where it appears**

To explain a batch, the app must combine manually:

- `current_lsn()`,
- `row_lsn(...)`,
- `entity_version(...)`,
- `entity_version_lsn(...)`,
- `resolve_reference(...)`,
- `FOR HISTORY`,
- `AS OF LSN`.

**Why this is ASQL friction**

These capabilities are part of the core product value, but composition still falls on the integrator.

**Adoption impact**

- it is hard to turn powerful primitives into daily playbooks,
- historical debugging feels expert-only instead of normal,
- every team ends up inventing its own temporal inspection language.

**Product opportunity**

- a more prescriptive cookbook,
- SDK helpers for snapshot + history + explanation,
- more guided Studio flows for multi-table temporal analysis.

**Priority**: P1

---

### 5. The fixture-first promise is right, but authoring cost is still high

**Where it appears**

The sample fixture is clear and deterministic, but writing it requires careful ordering of domains, dependencies, versioned references, and transaction steps.

**Why this is ASQL friction**

Strict determinism is a core product property.
That hardens the format and removes shortcuts common in informal seeding.

**Adoption impact**

- steeper learning curve for teams coming from SQL scripts or ORMs,
- difficulty maintaining large fixtures during schema changes,
- more up-front cost before the reproducibility payoff becomes obvious.

**Product opportunity**

- tooling to derive fixtures from controlled local scenarios,
- validations with more pedagogical feedback,
- fixture-first starter packs by integration type.

**Priority**: P1

---

### 6. Pgwire compatibility helps, but still forces teams to think actively about the supported subset

**Where it appears**

Go integration with `pgx` works, but the app still needs to assume a concrete PostgreSQL subset and details such as `simple_protocol`.

**Why this is ASQL friction**

The natural expectation is “if it speaks pgwire, my PostgreSQL stack should fit with little friction.”
Reality is more nuanced.

**Adoption impact**

- early uncertainty about which client or access layer is safe,
- more need for exploratory testing,
- additional friction for stacks that do not control the emitted SQL well.

**Product opportunity**

- a more actionable compatibility matrix,
- guides by client type,
- errors with concrete recommendations when the query leaves the supported subset.

**Priority**: P0

---

### 7. There is still no ergonomic middle layer between engine primitives and reusable integration patterns

**Where it appears**

The application ends up creating its own helpers to:

- start the correct transactional scopes,
- record `LSN` checkpoints,
- resolve historical snapshots,
- translate raw history into readable explanations.

**Why this is ASQL friction**

The engine exposes the primitives well, but still leaves too much repetitive assembly work to every team.
This is not purely a conceptual modeling problem; it is the lack of a reusable integration layer between raw SQL and everyday usage.

**Adoption impact**

- duplicated utilities across projects,
- less idiomatic integrations than necessary,
- longer time until the team feels it “knows how to use ASQL well.”

**Product opportunity**

- generic SDK helpers,
- non-vertical reference packages,
- recommended conventions for transaction scopes, temporal inspection, and historical explanation.

**Priority**: P1

---

### 8. The ASQL mental model demands more explicit responsibility and raises the initial adoption bar

**Where it appears**

The app has to decide:

- which domains participate,
- which tables should be entities,
- which snapshot a reference should capture,
- which state should be explained with row history and which with entity version,
- which temporal inspections belong in operational workflows.

**Why this is ASQL friction**

This is the natural cost of an engine that prioritizes determinism, history, and visible boundaries.
It is not the same as friction 1, which is about the specific transactional boundary, nor friction 7, which is about the lack of reusable helpers and patterns.
The issue here is the full mental-model shift the team must internalize.

**Adoption impact**

- slower onboarding than with a traditional relational database,
- more need for architectural coaching,
- risk of disappointment if the team expected only “Postgres with extras.”

**Product opportunity**

- a clearer narrative about the mental-model shift,
- adoption-specific training,
- deep reference apps like this one kept subordinate to the getting-started flow.

**Priority**: P0

---

### 9. Adoption observability still needs to close the loop better between modeling and runtime

**Where it appears**

The sample raises useful questions that still require manual investigation, for example:

- how many transactions cross too many domains,
- which versioned references capture more changes than expected,
- which entities generate the most versions per unit of work,
- where `AS OF LSN` and `FOR HISTORY` queries concentrate.

**Why this is ASQL friction**

These are key signals for knowing whether a team is using the engine model well, not only for operating the runtime.

**Adoption impact**

- slow feedback on bad modeling decisions,
- difficulty improving integrations iteratively,
- more dependence on manual code and fixture review.

**Product opportunity**

- temporal-adoption and cross-domain usage metrics,
- admin views oriented to modeling,
- diagnostics that connect schema shape with runtime patterns.

**Priority**: P2

---

### 10. Schema evolution becomes more delicate when the design already includes history, entities, and versioned references

**Where it appears**

As soon as the model includes `CREATE ENTITY` and `VERSIONED FOREIGN KEY`, any change in tables, relationships, or aggregate boundaries stops feeling like an ordinary SQL migration.

**Why this is ASQL friction**

In ASQL, schema shape affects not only current writes and reads.
It also affects how references are captured, how history is reconstructed, and how future snapshots are interpreted.

**Adoption impact**

- more caution when evolving the model,
- higher migration-review cost because temporal semantics can change even when the SQL does not look dramatically different,
- difficulty answering confidently which changes are safe for history, replay, and explainability.

**Product opportunity**

- specific guardrails for entity and versioned-reference evolution,
- migration checklists with explicit temporal impact,
- validations that warn when a migration effectively changes observable historical semantics.

**Priority**: P1

---

### 11. Modeling mistakes do not yet always turn into adoption diagnostics that are guided enough

**Where it appears**

When a versioned reference does not resolve as expected, when a `CROSS DOMAIN` looks excessive, or when an entity is poorly bounded, the team often needs a lot of manual interpretation to understand the real problem.

**Why this is ASQL friction**

ASQL onboarding depends heavily on learning its mental model.
If errors are expressed only as local technical failures instead of modeling feedback, learning slows down significantly.

**Adoption impact**

- more trial and error than necessary,
- greater dependence on internal experts or manual review,
- higher risk that the team blames the issue on “strange engine behavior” instead of a fixable modeling decision.

**Product opportunity**

- errors with more pedagogical context,
- diagnostics that suggest reviewing `ROOT`, `INCLUDES`, transactional scope, or the type of temporal reference,
- schema and fixture validations designed explicitly for onboarding.

**Priority**: P1

## What should not be solved by pushing pharma logic into ASQL

These frictions do not justify moving into the engine:

- the regulatory meaning of a signature,
- deviation or CAPA taxonomies,
- specific GMP/GAMP policies,
- Qualified Person semantics,
- organization-specific approval or evidence workflows.

Improvements should go into:

- general ergonomics,
- documentation,
- validation,
- observability,
- tooling,
- SDKs and integration patterns.

## Conclusion

The sample confirms that ASQL brings strong differentiated value when an application needs:

- explicit boundaries,
- reproducible snapshots,
- stable temporal references,
- queryable history,
- and a deterministic explanation of how a state came to exist.

The main friction is not pharma manufacturing itself.
It is the mental-model shift and the ergonomics needed for a team to use engine primitives quickly without inventing too much integration infrastructure.

The priority should be to reduce adoption friction without weakening ASQL's core properties.

## Executive summary

If this log had to be compressed into a small set of product-prioritization points, the reading would be:

- **P0**: make explicit boundaries, temporal schema design, and real pgwire compatibility easier to adopt.
- **P1**: reduce the cost of modeling entities, explaining history, evolving temporal schema, and building repeatable integration layers.
- **P2**: close the adoption-observability loop so the runtime also helps improve modeling.

The general thesis remains:

- ASQL already has strong primitives,
- the biggest remaining work is in ergonomics, diagnostics, patterns, and adoption narrative,
- and that work should stay general-purpose, not sector-specific.
