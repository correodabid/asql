# ADR 0002: Keep ASQL general-purpose while improving adoption ergonomics

- Status: Proposed
- Date: 2026-03-12
- Decision drivers:
  - preserve ASQL as a general-purpose deterministic SQL engine
  - avoid collapsing application semantics into the database product
  - reduce adoption friction without weakening core principles
  - clarify what ASQL should provide vs what applications must own

## Context

The recent adoption-friction review surfaced two truths at the same time:

1. ASQL exposes strong primitives for determinism, temporal inspection, replay, domain isolation, and audit-friendly workflows.
2. Some of the friction experienced by application teams is not a sign that ASQL should absorb more and more business logic into the engine.

This distinction matters.

ASQL is not a healthcare database, a finance database, or a manufacturing execution product. It is a general-purpose database with unusually strong guarantees around determinism, replay, temporal state, and explicit transaction boundaries.

That means some concerns are rightly in scope for the engine:

- explicit domain boundaries,
- deterministic transactions,
- temporal read/query primitives,
- entity/version capture support,
- fixture/replay/tooling ergonomics,
- compatibility and operational clarity.

But other concerns are rightly in scope for the application:

- workflow-specific state machines,
- e-signature meaning catalogs,
- business-specific approval models,
- actor/role semantics,
- domain-specific event vocabularies,
- case timelines and business projections,
- compliance interpretation for a specific regulation or product.

Without a clear boundary, ASQL risks moving in one of two bad directions:

- becoming too low-level for adoption,
- or becoming a domain opinionated platform instead of a compact general-purpose database.

## Decision

ASQL will remain a general-purpose deterministic SQL engine.

The product will improve adoption by adding better primitives, defaults, tooling, documentation, and integration patterns, but it will not absorb domain-specific application semantics into the core engine.

More specifically:

1. ASQL should invest in higher-quality general primitives and developer ergonomics.
2. ASQL should not embed workflow-specific business semantics into the engine.
3. ASQL should make application responsibilities explicit in docs and examples.
4. ASQL should ship better reference patterns for app-owned audit and workflow layers.
5. When a friction is identified, the first question must be:
   - should the engine own this capability, or
   - should ASQL provide a better primitive/pattern so the application can own it cleanly?

## Decision details

### 1. What ASQL should own

ASQL should own capabilities that are database-general and strengthened by determinism:

- explicit domain transaction boundaries,
- append-only truth and replay,
- `AS OF LSN` and temporal query semantics,
- stable `FOR HISTORY` contract,
- entity/version primitives,
- versioned-reference support,
- deterministic fixture workflows,
- compatibility policy and predictable SQL surface,
- observability for replay, replication, and temporal diagnostics.

### 2. What the application should own

Applications should own capabilities that are product-specific, organization-specific, or regulation-interpretation-specific:

- approval workflows,
- compliance vocabulary such as `reason`, `meaning`, evidence classes, and attestation policies,
- actor identity semantics and permission models,
- business event taxonomies,
- case management projections and timelines,
- domain-specific invariants that exceed generic relational/temporal rules,
- UX/UI behavior and workflow orchestration.

### 3. What ASQL should provide instead of owning business semantics

Where friction exists, ASQL should prefer:

- good primitives over domain DSLs,
- reference schemas over hard-coded vertical features,
- SDK helpers over engine magic where the concern is integration-shaped,
- docs and starter kits over product sprawl,
- generic metadata patterns over regulation-specific objects.

Examples:

- ASQL should support strong temporal introspection, but not ship a healthcare-only patient timeline engine.
- ASQL should support deterministic audit-friendly tables and helpers, but not prescribe a universal e-signature compliance model.
- ASQL should support explicit domain-crossing transactions, but not encode a business workflow engine.

### 4. Adoption guidance must include responsibility boundaries

Future docs and examples should explicitly label:

- engine-owned concern,
- app-owned concern,
- recommended integration pattern.

This will reduce false expectations and help design partners adopt ASQL with the right abstraction boundaries.

## Consequences

### Positive

- protects ASQL from vertical-product drift
- keeps the engine compact and more reusable across industries
- helps prioritize product work that scales across many use cases
- makes adoption conversations more honest
- improves examples without overloading the engine with app logic

### Negative / costs

- some users will still want more out-of-the-box workflow abstractions
- ASQL documentation must work harder to teach integration patterns
- the team must actively resist feature requests that belong in app land
- reference applications and starter kits become more important

### Neutral / preserved

- determinism remains non-negotiable
- domain isolation remains core to the product
- raw primitives remain available for advanced use cases
- ASQL can still be excellent for regulated industries without becoming a vertical product

## Alternatives considered

### Alternative A: Push more compliance/workflow semantics into the engine

Rejected.

This would make ASQL less general, increase surface area quickly, and couple the database to domain-specific interpretations that belong in application space.

### Alternative B: Keep the engine minimal and solve everything with docs only

Rejected.

Some adoption friction is real product friction and requires better primitives, helpers, tooling, and supported patterns.

### Alternative C: Offer separate vertical packs inside the core repository

Rejected for now.

This risks blurring ownership boundaries too early. Reference apps and examples are a better first step.

## Implementation guidance

Execution should follow this ordering:

1. make responsibility boundaries explicit in docs and ADRs
2. prioritize engine-general ergonomics that help many domains
3. provide reference patterns for app-owned audit/workflow layers
4. reject or defer requests that would turn ASQL into a vertical platform unless a capability is clearly database-general

## Acceptance signals

This ADR is successful when:

- adoption docs clearly distinguish engine-owned vs app-owned concerns
- roadmap prioritization favors reusable database capabilities over vertical features
- examples show how to build rich workflows without pretending the engine owns them
- design-partner feedback becomes more precise about what belongs in ASQL and what belongs in the app

## Related documents

- [docs/adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md](0001-engine-surface-dx-and-versioned-reference-ergonomics.md)
- [docs/getting-started/README.md](../getting-started/README.md)
- [docs/getting-started/01-overview.md](../getting-started/01-overview.md)
