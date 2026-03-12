# 00. Engine-Owned vs App-Owned Concerns

Read this note early.

ASQL adoption gets easier when teams separate two questions:

1. what the database should own,
2. and what the application should still own.

ASQL is a general-purpose deterministic SQL engine.
It should make rich applications easier to build and debug.
It should not become the place where product-specific workflow semantics live.

## ASQL should own

ASQL should own capabilities that are database-general and strengthened by determinism:

- explicit domain-scoped and cross-domain transaction boundaries,
- append-only WAL and replay semantics,
- `AS OF LSN`, `FOR HISTORY`, and temporal helper functions,
- entity/version primitives and versioned-reference capture,
- deterministic fixtures and replay-safe tooling,
- compatibility clarity, observability, and operator-facing diagnostics.

These are engine concerns because they improve many application types, not one vertical workflow.

## The application should own

The application should still own capabilities that are business-specific or regulation-interpretation-specific:

- approval workflows,
- actor and role semantics,
- business event taxonomies,
- compliance vocabulary and evidence meaning,
- product-specific case timelines and projections,
- orchestration across UI, APIs, and downstream systems.

These are application concerns even when ASQL stores the underlying facts.

## What ASQL should provide instead

When teams feel friction here, the default response should usually be one of these:

- stronger primitives,
- better defaults,
- clearer docs,
- reference patterns,
- SDK helpers,
- or starter-kit conventions.

That is usually better than pushing business-specific semantics into the engine.

## Feature-triage rubric

When deciding whether a capability belongs in ASQL, ask:

1. would this help many different applications, not just one business workflow?
2. is it fundamentally about deterministic data, history, transactions, replay, or database observability?
3. can it be expressed as a reusable primitive instead of a domain-specific workflow object?

If the answer is mostly yes, it may belong in ASQL.
If the answer is mostly no, it probably belongs in the application layer.

Use this rubric whenever a proposed feature sounds attractive because one example app needs it.

### Triage outcomes

- **Engine feature**: belongs in ASQL because it is database-general and improves deterministic behavior, history, replay, or operability.
- **Integration pattern**: belongs in docs, examples, SDK helpers, or starter kits rather than the core engine.
- **Application concern**: belongs in service code, product workflows, or UI/API orchestration.

## Example boundary

- ASQL should help you capture a replay-safe historical transfer snapshot.
- The application should decide what “approved”, “flagged”, or “requires escalation” means.

- ASQL should help you store deterministic audit-friendly events and inspect row/entity history.
- The application should decide what legal or organizational meaning those events carry.

## Why this note matters

Teams usually struggle more when they expect ASQL to be:

- just PostgreSQL with a few extras, or
- a workflow platform that absorbs business semantics.

It is neither.

The adoption sweet spot is:

- keep business workflow ownership in the app,
- use ASQL to make boundaries, history, determinism, and replay first-class.

## Next step

Continue with [01-overview.md](01-overview.md).