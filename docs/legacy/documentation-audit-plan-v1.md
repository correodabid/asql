# ASQL documentation audit plan v1

## Goal

Review the full documentation surface of ASQL so that:

- nothing important is out of date,
- the structure is easier to navigate,
- getting-started becomes the main onboarding spine,
- README, guides, examples, ADRs, and AI instructions remain aligned,
- obsolete or duplicated content is either merged, archived, or removed.

Status update (2026-03-12):

- the main audit and restructuring goals in this plan were completed in the current cleanup pass,
- the remaining work is incremental review on a small residual set of docs rather than broad structural remediation.

Status update (2026-03-13):

- this file is now primarily historical context for maintainers,
- [documentation-classification-matrix-v1.md](../maintenance/documentation-classification-matrix-v1.md) should be treated as the main active inventory for future minimization decisions,
- this plan moved to [docs/legacy/README.md](README.md) because maintainers no longer need the original audit narrative in the active maintenance set.

## Review scope

This audit includes:

- root README
- [docs/getting-started/](../getting-started)
- quickstart and cookbook material
- migration and compatibility docs
- architecture / operations / pricing / support docs
- ADRs
- AI planning docs under [docs/ai/](../ai)
- [.github/copilot-instructions.md](../../.github/copilot-instructions.md)
- example app docs such as [hospitalapp/README.md](../../hospitalapp/README.md)

## Operating principles

1. Getting-started is the primary narrative for adoption.
2. README is the short front door, not a second full guide.
3. Standalone docs should exist only when they are:
   - architectural,
   - policy/reference-oriented,
   - or too deep for onboarding.
4. AI/copilot instructions must reflect the current product state, not an early-MVP snapshot.
5. If two docs teach the same thing, one should become primary and the other should be reduced, linked, or removed.

## Initial issues identified at audit start

### A. Copilot instructions were partially stale

Likely issues:

- they still frame large parts of the project as `MVP`,
- the repo-shape section reads like a future target rather than the current repository reality,
- scope notes may lag behind implemented features already present in the codebase.

Resolution in this pass:

- rewrite instructions around current product state,
- keep non-negotiable principles,
- remove or reframe outdated roadmap language.

### B. Quickstart content was duplicated across multiple places

Likely overlap between:

- root README
- [docs/getting-started/README.md](../getting-started/README.md)
- [docs/getting-started/10-min.md](../getting-started/10-min.md)

Resolution in this pass:

- define one primary short-path,
- keep README minimal,
- make getting-started the main source of truth,
- reduce duplication of commands and explanations.

### C. Port/runtime examples needed consistency review

Examples currently reference different startup shapes and port conventions.

Resolution in this pass:

- choose canonical local-start examples,
- align README, getting-started, quickstart, examples, and Studio instructions.

### D. Adoption documentation needed consolidation

Now that we have:

- [hospitalapp/FRICTION_LOG.md](../../hospitalapp/FRICTION_LOG.md)
- [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](../adr/0002-generalist-engine-boundary-and-adoption-surface.md)
- [docs/product/asql-adoption-friction-prioritized-backlog-v1.md](../product/asql-adoption-friction-prioritized-backlog-v1.md)

we should ensure they complement rather than repeat each other.

Resolution in this pass:

- keep ADR for boundary decision,
- keep backlog for execution,
- keep friction log as evidence/example input,
- avoid restating the same narrative in three places.

## Proposed target structure

All Markdown files under [docs/](..)
should live inside a classified folder.
There should be no loose `.md` files directly at the root of [docs/](..).

### 1. Front door

- [README.md](../../README.md)
  - short product pitch
  - one minimal quickstart
  - clear link to getting-started
  - clear link to compatibility and operational docs

### 2. Primary onboarding

- [docs/getting-started/README.md](../getting-started/README.md)
- chapterized path in [docs/getting-started/](../getting-started)

This should absorb most developer-learning material.

### 3. Deeper developer references

- [docs/reference/](../reference)
  - cookbook
  - fixture format
  - temporal introspection surface
  - aggregate/version semantics
  - compatibility docs

These should support getting-started, not replace it.

### 4. Product / architecture / ops / commercial docs

- [docs/architecture/](../architecture)
- [docs/operations/](../operations)
- [docs/product/](../product)
- [docs/commercial/](../commercial)

### 5. Decision records

- [docs/adr/](../adr) only for durable decisions

### 6. AI / maintenance instructions

- [docs/ai/](../ai)
- [docs/maintenance/](../maintenance)
- [.github/copilot-instructions.md](../../.github/copilot-instructions.md)

These should align with the actual current repository and current product stance.

### 7. Legacy review area

- [docs/legacy/](.)

This folder is for historical docs or material under review for deletion.
Anything moved there should be treated as non-primary documentation.

## Audit workflow

### Phase 1 — Inventory and classification

For each doc, classify it as:

- keep as primary
- keep as reference
- merge into getting-started
- merge into another doc
- archive
- delete
- rewrite from scratch

### Phase 2 — Source-of-truth mapping

For each major topic, define a single primary doc:

- installation
- first run
- domains / transactions
- temporal reads
- fixtures
- Go integration
- compatibility
- migration
- observability / operations
- pricing / support
- architecture
- AI instructions

### Phase 3 — Consistency pass

Check all docs for:

- outdated feature/scope language
- stale ports / commands / flags
- references to superseded workflows
- duplicated onboarding text
- broken links
- mismatched terminology

### Phase 4 — Removal and rewrite pass

- remove duplicates
- shrink overlapping docs into pointers
- rewrite stale docs
- update AI instructions

## Residual execution candidates

1. Audit and update [.github/copilot-instructions.md](../../.github/copilot-instructions.md)
2. Unify README + getting-started + 10-min guide
3. Define canonical local startup and port examples
4. Decide whether `docs/getting-started/10-min.md` stays, shrinks, or merges into getting-started
5. Add a classification table for the full docs corpus

## Acceptance criteria

The audit is successful when:

- getting-started is clearly the main adoption path,
- README no longer duplicates large sections of getting-started,
- no major doc still describes ASQL as if it were only at an early MVP state when the repo has moved beyond that,
- copilot instructions reflect current reality,
- every major topic has one clear source of truth,
- obviously redundant documents are removed or reduced to links.
