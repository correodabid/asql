# ASQL documentation cleanup backlog v1

## Goal

Turn the documentation audit into an execution sequence that removes stale content, reduces duplication, and makes getting-started the main adoption spine.

Status update (2026-03-12):

- the structural and freshness cleanup pass is complete,
- the residual review items from this audit have also been completed,
- this file now serves mainly as a record of what was done and what acceptance gates were used.

Status update (2026-03-13):

- the next documentation goal is minimization rather than expansion,
- user-facing onboarding should keep one primary path per intent,
- any companion doc should exist only when it is materially shorter or materially deeper than the primary doc.
- this file is now best treated as historical cleanup record rather than an active maintainer control surface,
- [documentation-classification-matrix-v1.md](../maintenance/documentation-classification-matrix-v1.md) should be the primary active minimization inventory going forward,
- this file moved to [docs/legacy/README.md](README.md) after the cleanup history stopped being part of the active maintenance surface.

Structural rule:

- no loose Markdown files directly inside [docs/](..),
- every active doc must live in a classified subfolder,
- anything pending deletion review belongs in [docs/legacy/](.).

Minimization rule:

- one primary document per audience + task,
- companion docs must defer quickly to the primary path,
- if two docs serve the same audience and the same job, merge one into the other or move one toward archival review.

## Priority model

- `P0`: directly blocks clarity or causes users/agents to learn the wrong thing
- `P1`: high-value cleanup that reduces duplication and structural confusion
- `P2`: deeper editorial and archival cleanup

## Next reduction pass

These are the main candidates for future consolidation if duplication starts growing again:

1. the former `docs/getting-started/10-min.md`
	- was merged away once README plus getting-started covered the same quick path.
2. the former `docs/getting-started/09a-general-purpose-starter-pack.md`
	- was folded into [docs/getting-started/10-adoption-playbook.md](../getting-started/10-adoption-playbook.md).
3. the former `docs/migration/sqlite-quick-path.md`
	- was folded into [docs/migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md).
4. the former `docs/getting-started/04a-domain-modeling-guide.md`
	- was folded into [docs/getting-started/04-domains-and-transactions.md](../getting-started/04-domains-and-transactions.md).
5. the former `docs/getting-started/00-engine-owned-vs-app-owned.md`
	- was folded into [docs/getting-started/README.md](../getting-started/README.md) and [docs/getting-started/01-overview.md](../getting-started/01-overview.md).

## P0 — Fix misleading or stale sources of truth

### 1. Rewrite [.github/copilot-instructions.md](../../.github/copilot-instructions.md)

Completed.

Problem:

- still partly framed as an earlier MVP/future-shape document
- may mislead AI agents about current product/runtime reality

Deliverables:

- current product-state rewrite
- retain durable principles
- remove stale repo-shape and outdated scope language

### 2. Rewrite [README.md](../../README.md) as a short front door

Completed.

Problem:

- overlaps with getting-started
- currently acts as a second onboarding guide

Deliverables:

- short product pitch
- one canonical quickstart path
- direct handoff to [docs/getting-started/README.md](../getting-started/README.md)
- direct links to compatibility, migration, and operations docs

### 3. Canonicalize startup/runtime examples

Completed for the current docs surface.

Problem:

- inconsistent ports, startup commands, and runtime wording across docs
- [docs/operations/runbook.md](../operations/runbook.md) likely has stale flag descriptions

Deliverables:

- choose canonical local-start examples
- update README, getting-started, runbook, cookbook, and example references
- verify Studio startup syntax

### 4. Decide the fate of the former `docs/getting-started/10-min.md`

Completed in a later pass: removed after its quick-path content was fully covered by README and [docs/getting-started/README.md](../getting-started/README.md).

Problem:

- duplicates getting-started and README fast paths

Deliverables:

- either merge into getting-started fast path
- or shrink into a minimal companion doc with no duplicated explanations

## P1 — Consolidate onboarding and adoption docs

### 5. Strengthen [docs/getting-started/README.md](../getting-started/README.md) as the narrative spine

Completed.

Deliverables:

- clearer fast paths
- explicit routing to deeper references
- adoption boundary callouts

### 6. Fold adoption-friction lessons into getting-started chapters

Completed for the current adoption spine.

Targets:

- [docs/getting-started/04-domains-and-transactions.md](../getting-started/04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](../getting-started/05-time-travel-and-history.md)
- [docs/getting-started/07-fixtures-and-seeding.md](../getting-started/07-fixtures-and-seeding.md)
- [docs/getting-started/09-go-sdk-and-integration.md](../getting-started/09-go-sdk-and-integration.md)
- [docs/getting-started/10-adoption-playbook.md](../getting-started/10-adoption-playbook.md)
- [docs/getting-started/11-troubleshooting.md](../getting-started/11-troubleshooting.md)

Deliverables:

- clearer engine-owned vs app-owned boundaries
- common expectation mismatches
- fixture-first onboarding emphasis
- transaction-helper integration guidance

### 7. Reclassify deep docs as references, not onboarding

Completed.

Targets:

- [docs/reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md)
- [docs/reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md)
- [docs/reference/temporal-introspection-surface-v1.md](../reference/temporal-introspection-surface-v1.md)
- [docs/reference/aggregate-reference-semantics-v1.md](../reference/aggregate-reference-semantics-v1.md)

Deliverables:

- short intro in each doc explaining that getting-started is primary
- reduced overlap with onboarding chapters

### 8. Normalize adoption docs around current boundary decision

Completed.

Targets:

- [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](../adr/0002-generalist-engine-boundary-and-adoption-surface.md)
- [docs/product/asql-adoption-friction-prioritized-backlog-v1.md](../product/asql-adoption-friction-prioritized-backlog-v1.md)
- [hospitalapp/FRICTION_LOG.md](../../hospitalapp/FRICTION_LOG.md)

Deliverables:

- no repeated narrative across all three
- each doc has one distinct purpose

## P2 — Archive, prune, and mark historical docs

### 9. Remove superseded planning docs after legacy review

Completed:

- deleted the superseded engineering DX backlog and improvement-plan notes,
- deleted the early AI roadmap,
- kept [docs/legacy/README.md](README.md) as the retention-policy marker for future review cycles.

Follow-through:

- remove or rewrite any leftover references when future legacy reviews happen

### 10. Audit policy/ops/commercial docs for freshness

Completed for the current pass.

Targets:

- runbook, incident, SLO, telemetry, release checklist, pricing, support, security, architecture, benchmark docs

Deliverables:

- each doc marked current or rewritten
- stale claims removed
- broken assumptions corrected

### 11. Audit [docs/ai/](../ai) as an internal documentation set

Completed.

Additional close-out completed after the main pass:

- refreshed [docs/reference/versioned-reference-capture-semantics-v1.md](../reference/versioned-reference-capture-semantics-v1.md),
- later folded the short SQLite migration companion into [docs/migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md),
- refreshed [docs/ai/06-agent-playbook.md](../ai/06-agent-playbook.md), [docs/ai/07-definition-of-done.md](../ai/07-definition-of-done.md), and [docs/ai/09-benchmark-baseline.md](../ai/09-benchmark-baseline.md),
- added an implementation-status note to [docs/adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md](../adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md),
- positioned [hospitalapp/README.md](../../hospitalapp/README.md) explicitly as a supporting example doc.

Deliverables:

- separate current internal docs from historical planning docs
- make sure AI instructions, backlog, architecture, and definition-of-done agree

## Acceptance gates

- [x] README no longer behaves like a second full onboarding path
- [x] getting-started is the obvious primary adoption route
- [x] copilot instructions reflect current product reality
- [x] runtime/startup examples are consistent across docs
- [x] stale planning docs are marked historical, archived, or removed
- [x] deep reference docs support onboarding instead of duplicating it
