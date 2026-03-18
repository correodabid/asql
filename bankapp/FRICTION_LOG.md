# BankApp – ASQL Friction Log

## Objective

This document captures frictions observed while building a reference application that uses ASQL intensively.

The focus is strictly technological:

- engine friction,
- mental-model friction,
- SQL/pgwire surface friction,
- integration ergonomics,
- and operational workflow friction.

Banking-domain friction is intentionally out of scope.

## What the sample deliberately forced

The sample deliberately uses:

- 4 explicit domains,
- `BEGIN DOMAIN ...` and `BEGIN CROSS DOMAIN ...` transactions,
- entities (`CREATE ENTITY`),
- `VERSIONED FOREIGN KEY`,
- temporal helpers,
- `FOR HISTORY`,
- `AS OF LSN`,
- deterministic fixtures,
- Go integration via pgwire with `pgx`.

That makes it visible where a team will struggle during the first weeks of adoption.

## Observed frictions

### 1. Domain selection enters application code very early

**Where it appears**

As soon as the first use case spans more than one boundary (`identity`, `ledger`, `payments`, `risk`), the application must decide on every operation whether it uses `DOMAIN` or `CROSS DOMAIN`.

**Why this is ASQL friction**

This is not a quirk of the sample. It is part of ASQL's central contract: boundaries are not implicit.

**Adoption impact**

- forces teams to create transaction helpers from the beginning,
- breaks repositories or services that assumed invisible transactions,
- pushes application-layer redesign before the team masters the rest of the capabilities.

**Product opportunity**

- official patterns for per-domain transaction helpers,
- diagnostics for `CROSS DOMAIN` overuse,
- a short guide for discovering the right boundaries.

**Priority**: P0

---

### 2. The temporal model introduces new schema concepts, not only new queries

**Where it appears**

Versioned references require explicit columns such as `customer_version`, `source_account_version`, or `transfer_version`.

**Why this is ASQL friction**

The temporal capability does not live only at runtime. It affects physical table design and migrations.

**Adoption impact**

- the team must learn when to store `LSN` versus entity version,
- technical columns appear that do not exist in conventional SQL schemas,
- it becomes harder to explain to product teams why there are “extra” fields that the engine manages indirectly.

**Product opportunity**

- schema templates for versioned references,
- comparative guidance: ordinary row vs entity vs versioned reference,
- validations or suggestions when defining `VERSIONED FOREIGN KEY`.

**Priority**: P0

---

### 3. `CREATE ENTITY` adds a lot of value, but demands clear upfront modeling

**Where it appears**

The application must decide which table is `ROOT` and which tables belong in `INCLUDES` before version capture becomes useful.

**Why this is ASQL friction**

ASQL offers a strong aggregate layer, but teams coming from purely relational modeling do not have an easy transition path.

**Adoption impact**

- can delay entity adoption for too long,
- or produce badly defined entities that later contaminate temporal semantics,
- introduces a strong architectural decision very early.

**Product opportunity**

- entity-modeling guidance with multi-industry examples,
- a checklist for deciding when a table should be `ROOT`,
- tooling or validations for suspiciously large or empty entities.

**Priority**: P1

---

### 4. Temporal debugging is powerful, but still too manual

**Where it appears**

The sample has to combine manually:

- `current_lsn()`,
- `row_lsn(...)`,
- `entity_version(...)`,
- `entity_version_lsn(...)`,
- `AS OF LSN`,
- `FOR HISTORY`.

**Why this is ASQL friction**

All of these pieces belong to the core product value, but composition still falls almost entirely on the integrator.

**Adoption impact**

- increases the time until teams get the real benefit of replay/history,
- makes it harder to standardize incident playbooks,
- makes new teams perceive temporal capability as “expert-only” instead of normal.

**Product opportunity**

- a cookbook with “current state + historical explanation” patterns,
- SDK helpers for snapshot, history, and version resolution,
- more prescriptive onboarding examples.

**Priority**: P1

---

### 5. The fixture-first flow is correct, but fixture authoring is still expensive

**Where it appears**

The sample fixture is explicit, ordered, and useful, but writing it by hand requires a lot of detail.

**Why this is ASQL friction**

ASQL requires real determinism. That hardens the format and removes common shortcuts from ad hoc seeds.

**Adoption impact**

- large fixtures can feel expensive to maintain,
- the transition from SQL scripts or ORM seeds is abrupt,
- the team must learn an additional discipline for tests and demos.

**Product opportunity**

- better tooling to derive fixtures from controlled local environments,
- validation messages aimed more clearly at learning,
- fixture-first starter packs for new projects.

**Priority**: P1

---

### 6. Pgwire compatibility is useful, but the supported surface still requires active vigilance

**Where it appears**

Go integration works well with `pgx`, but it still forces teams to think about the real PostgreSQL subset and details such as `simple_protocol`.

**Why this is ASQL friction**

The natural expectation will be “if it speaks pgwire, my PostgreSQL stack should work.” In practice, the final fit depends on the supported subset.

**Adoption impact**

- early uncertainty about which client, ORM, or SQL pattern is safe,
- a need to discover surprises through trial and error,
- higher integration cost for non-Go stacks.

**Product opportunity**

- a more actionable compatibility matrix,
- guidance per client type,
- guardrails and errors with concrete recommendations.

**Priority**: P0

---

### 7. ASQL shows its value when the app takes explicit responsibility, and that raises the initial bar

**Where it appears**

The application must decide:

- which domains participate,
- which mutations belong together,
- which snapshots to retain mentally,
- which tables deserve entities,
- which data should be observed through history and which through app-owned auditing.

**Why this is ASQL friction**

This is not a flaw in the sample. It is the direct consequence of an engine that makes boundaries, determinism, and history explicit.

**Adoption impact**

- slower onboarding than with a traditional relational database,
- more need for architectural coaching,
- risk of early rejection if the team expected “Postgres with extras”.

**Product opportunity**

- a clearer narrative around engine responsibility vs app responsibility,
- training material for the mental model,
- reference apps that stay subordinate to the getting-started flow.

**Priority**: P0

---

### 8. There is still no ergonomic middle layer between raw SQL and advanced capabilities

**Where it appears**

To use ASQL well, the application ends up building its own utilities to:

- start the right transactions,
- record `LSN` checkpoints,
- run temporal queries,
- turn history into readable explanations.

**Why this is ASQL friction**

The engine exposes the primitives well, but still leaves too much repetitive assembly to each team.

**Adoption impact**

- helper duplication across projects,
- inconsistency between teams,
- more time before integrations feel idiomatic.

**Product opportunity**

- generic SDK helpers,
- non-vertical reference packages,
- recommended conventions for IDs, timestamps, auditing, and temporal reads.

**Priority**: P1

## What should not be solved by pushing banking logic into ASQL

These frictions do not justify moving into the engine:

- risk scoring rules,
- regulatory compliance semantics,
- banking approval workflows,
- business event catalogs,
- actor, role, or case models.

The improvements should go into:

- general ergonomics,
- documentation,
- validation,
- observability,
- tooling,
- SDKs and integration patterns.

## Conclusion

The sample confirms that ASQL offers real differentiated value when an application needs:

- explicit boundaries,
- reproducible snapshots,
- queryable history,
- temporal references,
- and deterministic debugging.

The main friction is not the banking case itself. It is the mental-model shift and the ergonomics required for a team to use these capabilities quickly without reinventing patterns in every project.

The priority should be to reduce adoption friction without weakening the engine's core properties.
