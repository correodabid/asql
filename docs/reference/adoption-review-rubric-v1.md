# Adoption review rubric v1

Use this rubric when an example app or onboarding exercise exposes friction.
The goal is to decide whether the right answer is:

- engine work,
- docs,
- SDK/helper patterns,
- tooling,
- or application refactoring.

## 1. Engine work

Choose engine work when the friction is fundamentally about:

- deterministic transaction scope,
- history/replay correctness,
- temporal reference semantics,
- stable query/runtime surface,
- operator-visible safety signals.

Typical examples:

- missing diagnostics for `VERSIONED FOREIGN KEY` failures,
- missing supported surface for inspecting temporal state,
- missing admin visibility for adoption-critical runtime patterns.

## 2. Docs or training work

Choose docs/training when the primitive exists but teams still cannot adopt it predictably.

Typical examples:

- teams cannot distinguish row-head `LSN` from entity-version capture,
- teams assume pgwire means full PostgreSQL parity,
- teams do not know when to use `FOR HISTORY` versus `entity_version_lsn(...)`.

## 3. SDK/helper-pattern work

Choose SDK/helper-pattern work when multiple teams keep rebuilding the same thin integration layer.

Typical examples:

- repeated `BEGIN DOMAIN ...` wrappers,
- repeated `current -> history -> AS OF LSN -> explanation` flows,
- repeated version-to-`LSN` bridge helpers.

## 4. Tooling work

Choose tooling work when the engine surface is correct but review or debugging is still too manual.

Typical examples:

- schema diff should warn that entity or versioned-reference changes alter historical semantics,
- Studio or CLI should guide a historical explanation workflow,
- fixture validation should point teams toward model fixes, not just syntax fixes.

## 5. Application work

Choose application work when the friction is really about:

- business vocabulary,
- workflow orchestration,
- actor meaning,
- compliance interpretation,
- product-specific lifecycle design.

These should not be pushed into ASQL just because the example app made them visible.

## Quick decision rule

Ask:

1. Is the issue about database-general determinism, history, replay, or explicit boundaries?
2. Would fixing it help many application shapes, not just one vertical workflow?
3. Is the current friction caused by a missing primitive, a missing explanation, or a missing pattern?

Interpretation:

- mostly primitive/surface problem -> engine,
- mostly explanation problem -> docs/training,
- mostly repeated thin glue code -> SDK/helper patterns,
- mostly review/debug experience -> tooling,
- mostly domain meaning -> app-owned.
