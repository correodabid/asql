# ASQL documentation classification matrix v1

## Purpose

Provide an initial classification of the current documentation corpus so the project can:

- identify the primary source of truth for each topic,
- spot stale or duplicated content,
- decide what to keep, merge, rewrite, archive, or delete.

Status update (2026-03-12):

- the structural cleanup is complete,
- root-level loose docs under [docs/](../) have been eliminated,
- legacy planning docs reviewed in this pass were removed,
- the residual review queue from this audit pass has been closed.

## Action labels

- `keep-primary`: keep as the main source of truth for the topic
- `keep-reference`: keep as secondary/deeper reference
- `rewrite`: keep the topic but rewrite substantially
- `merge`: merge into another primary doc and reduce/remove duplication
- `archive/delete`: remove from active navigation once content is merged or superseded
- `review`: likely keep, but needs freshness/consistency verification

## Current summary

Resolved in this cleanup pass:

1. [README.md](../README.md), [.github/copilot-instructions.md](../.github/copilot-instructions.md), and the getting-started spine were refreshed.
2. Runtime/port wording was aligned around the pgwire-first `cmd/asqld` path.
3. Historical planning docs were removed or downgraded to clearly labeled strategy snapshots.
4. There are no loose Markdown files directly under [docs/](../); the taxonomy is now enforced.

Audit close-out:

1. the remaining reference, migration, AI-internal, ADR, and example-app docs were refreshed or explicitly positioned,
2. quick-path docs that remain are now intentionally subordinate rather than accidental duplicates,
3. any future changes should be handled as normal documentation maintenance rather than as part of this one-time cleanup pass.

## Matrix

| Path | Audience | Current role | Action | Notes |
|---|---|---|---|---|
| [.github/copilot-instructions.md](../.github/copilot-instructions.md) | AI/maintainers | Internal instructions | keep-primary | Rewritten to current product/runtime posture; active source of truth for AI-facing repo guidance. |
| [README.md](../README.md) | All users | Front door + minimal quickstart | keep-primary | Now acts as the short front door and routes adoption into getting-started. |
| [docs/getting-started/README.md](getting-started/README.md) | New adopters | Onboarding index | keep-primary | Should remain the main narrative spine. |
| [docs/getting-started/10-min.md](../getting-started/10-min.md) | New adopters | Short subordinate quick path | keep-reference | Retained as a minimal fast path that explicitly defers to the main getting-started spine. |
| [docs/getting-started/01-overview.md](getting-started/01-overview.md) | New adopters | Product intro | keep-primary | Good placement; needs boundary callouts and alignment with current stance. |
| [docs/getting-started/02-install-and-run.md](getting-started/02-install-and-run.md) | New adopters | Local startup | keep-primary | Canonical runtime/port/Studio invocation was refreshed in this cleanup pass. |
| [docs/getting-started/03-first-database.md](getting-started/03-first-database.md) | New adopters | First schema/read/write | keep-primary | Current pgwire-shell walkthrough remains aligned with the canonical local path. |
| [docs/getting-started/04-domains-and-transactions.md](getting-started/04-domains-and-transactions.md) | New adopters | Core mental model | keep-primary | Should absorb more adoption-boundary guidance. |
| [docs/getting-started/05-time-travel-and-history.md](getting-started/05-time-travel-and-history.md) | New adopters | Temporal intro | keep-primary | Good candidate to absorb more from temporal reference docs. |
| [docs/getting-started/06-entities-and-versioned-references.md](getting-started/06-entities-and-versioned-references.md) | New adopters | Aggregate/version intro | keep-primary | Keep; verify wording matches current automatic capture semantics. |
| [docs/getting-started/07-fixtures-and-seeding.md](getting-started/07-fixtures-and-seeding.md) | New adopters | Deterministic setup | keep-primary | Important onboarding asset; should become stronger. |
| [docs/getting-started/08-studio-cli-and-daily-workflow.md](getting-started/08-studio-cli-and-daily-workflow.md) | New adopters | Tooling workflow | keep-primary | Studio and `asqlctl` workflow stays aligned with the current pgwire-first posture. |
| [docs/getting-started/09-go-sdk-and-integration.md](getting-started/09-go-sdk-and-integration.md) | Go developers | Integration path | keep-primary | Good place for helper-pattern improvements. |
| [docs/getting-started/10-adoption-playbook.md](getting-started/10-adoption-playbook.md) | Teams adopting ASQL | Rollout guidance | keep-primary | Should absorb some new adoption-friction guidance. |
| [docs/getting-started/11-troubleshooting.md](getting-started/11-troubleshooting.md) | New adopters | Troubleshooting | keep-primary | Should receive expectation-mismatch material. |
| [docs/reference/cookbook-go-sdk.md](../reference/cookbook-go-sdk.md) | Go developers | Deep examples | keep-reference | Support getting-started; avoid duplicating onboarding narrative. |
| [docs/reference/fixture-format-and-lifecycle-v1.md](../reference/fixture-format-and-lifecycle-v1.md) | Developers/operators | Fixture reference | keep-reference | Good as deeper spec; getting-started should carry short path. |
| [docs/reference/temporal-introspection-surface-v1.md](../reference/temporal-introspection-surface-v1.md) | Developers | Temporal helper reference | keep-reference | Useful reference; avoid duplicating core intro already in getting-started. |
| [docs/reference/aggregate-reference-semantics-v1.md](../reference/aggregate-reference-semantics-v1.md) | Developers/architects | Semantics note | keep-reference | Keep as deeper conceptual note. |
| [docs/reference/versioned-reference-capture-semantics-v1.md](../reference/versioned-reference-capture-semantics-v1.md) | Developers/architects | Semantics note | keep-reference | Retained as a deeper standalone note with explicit links back to the primary onboarding/reference path. |
| [docs/reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md) | All users | Compatibility policy | keep-primary | Clear policy doc; README/getting-started should link to it. |
| [docs/reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md) | Developers | Detailed compatibility matrix | keep-reference | Good detailed companion to policy doc. |
| [docs/migration/sqlite-quick-path.md](../migration/sqlite-quick-path.md) | Migration users | Fast migration note | keep-reference | Retained as the short migration companion, with links to the fuller migration and adoption guides. |
| [docs/migration/sqlite-postgres-lite-guide-v1.md](../migration/sqlite-postgres-lite-guide-v1.md) | Migration users | Full migration guide | keep-reference | Should remain deeper migration document. |
| [docs/operations/runbook.md](../operations/runbook.md) | Operators/developers | Technical runbook | keep-primary | Current-runtime wording was aligned around the pgwire-first path. |
| [docs/operations/incident-runbook-v1.md](../operations/incident-runbook-v1.md) | Operators | Incident handling | keep-reference | Refreshed for pgwire-first auth/runtime posture; retain as an operational companion doc. |
| [docs/architecture/cluster-control-plane-note-v1.md](../architecture/cluster-control-plane-note-v1.md) | Architects/operators | Cluster internals | keep-reference | Keep as architectural/operator note. |
| [docs/product/production-readiness-roadmap-v1.md](../product/production-readiness-roadmap-v1.md) | Product/engineering | Current production roadmap | keep-primary | Appears current and explicitly supersedes older assumptions. |
| [docs/operations/release-upgrade-compat-checklist-v1.md](../operations/release-upgrade-compat-checklist-v1.md) | Release engineering | Release checklist | keep-reference | Updated to treat pgwire/SQL as the canonical contract surface and gRPC as optional. |
| [docs/operations/slo-v1.md](../operations/slo-v1.md) | Operators/product | Service objectives | keep-reference | Clarifies product-health/DX SLOs versus customer-runtime commitments. |
| [docs/operations/telemetry-dashboard-v1.md](../operations/telemetry-dashboard-v1.md) | Operators | Dashboard guidance | keep-reference | Refreshed to distinguish internal DX telemetry from customer-runtime telemetry. |
| [docs/product/benchmark-one-pager-v1.md](../product/benchmark-one-pager-v1.md) | Product/engineering | Benchmark summary | keep-reference | Current benchmark summary with clearer application-facing interpretation guidance. |
| [docs/product/performance-benchmark-plan-v1.md](../product/performance-benchmark-plan-v1.md) | Engineering | Benchmark plan | keep-reference | Still active as the layered benchmark-planning document. |
| [docs/architecture/architecture-one-pager-v1.md](../architecture/architecture-one-pager-v1.md) | Product/partners | Architecture summary | keep-reference | Refreshed to reflect `cmd/asqld` + pgwire as the canonical runtime path. |
| [docs/commercial/pricing-licensing-model-v1.md](../commercial/pricing-licensing-model-v1.md) | Commercial | Pricing/licensing | keep-reference | Refreshed with explicit product-boundary and runtime-posture guardrails. |
| [docs/commercial/support-policy-v1.md](../commercial/support-policy-v1.md) | Commercial/ops | Support policy | keep-reference | Refreshed with clear ASQL-vs-application support boundaries. |
| [docs/operations/security-disclosure-policy-v1.md](../operations/security-disclosure-policy-v1.md) | External/security | Security policy | keep-primary | Policy doc; keep unless superseded. |
| [docs/product/design-partner-triage-v1.md](../product/design-partner-triage-v1.md) | Product | Feedback operations | keep-reference | Refreshed to force ownership classification and avoid vertical workflow asks leaking into core scope. |
| [docs/commercial/comercial-caso-uso-fabricacion-farmaceutica.md](../commercial/comercial-caso-uso-fabricacion-farmaceutica.md) | Commercial | Specific vertical narrative | keep-reference | Retained as a sales/partner narrative with explicit note that ASQL remains a general-purpose database. |
| [docs/product/asql-adoption-friction-prioritized-backlog-v1.md](../product/asql-adoption-friction-prioritized-backlog-v1.md) | Product/engineering | Current adoption backlog | keep-primary | Current execution doc; keep. |
| [docs/maintenance/documentation-audit-plan-v1.md](documentation-audit-plan-v1.md) | Maintainers | Audit plan | keep-primary | New umbrella plan for this cleanup effort. |
| [docs/adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md](adr/0001-engine-surface-dx-and-versioned-reference-ergonomics.md) | Maintainers | Decision record | keep-reference | Now includes an implementation-status note; keep as rationale/history rather than live execution guidance. |
| [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](adr/0002-generalist-engine-boundary-and-adoption-surface.md) | Maintainers | Decision record | keep-primary | Current boundary decision; keep. |
| [docs/ai/README.md](ai/README.md) | AI/maintainers | Internal index | keep-primary | Updated to distinguish current core docs from historical docs. |
| [docs/ai/01-product-vision.md](ai/01-product-vision.md) | AI/maintainers | Product framing | keep-reference | Refreshed with pgwire-first posture and explicit note that the backlog is the active execution source. |
| [docs/ai/02-architecture-blueprint.md](ai/02-architecture-blueprint.md) | AI/maintainers | Internal architecture summary | keep-reference | Refreshed to match the current runtime and transport posture. |
| [docs/ai/03-transaction-and-protocol-spec.md](ai/03-transaction-and-protocol-spec.md) | AI/maintainers | Internal spec | keep-reference | Retained as an MVP-era conceptual spec with the current canonical runtime surface called out. |
| [docs/ai/05-backlog.md](ai/05-backlog.md) | AI/maintainers | Current execution backlog | keep-primary | Still active. |
| [docs/ai/06-agent-playbook.md](ai/06-agent-playbook.md) | AI/maintainers | Internal workflow | keep-reference | Still current; now explicitly linked to the active backlog and runtime constraints. |
| [docs/ai/07-definition-of-done.md](ai/07-definition-of-done.md) | AI/maintainers | Internal standards | keep-reference | Still current; explicitly paired with backlog and runtime constraints. |
| [docs/ai/08-productization-and-gtm-roadmap.md](ai/08-productization-and-gtm-roadmap.md) | AI/maintainers | Internal roadmap snapshot | keep-reference | Explicitly marked as a strategy snapshot; not the active execution plan. |
| [docs/ai/09-benchmark-baseline.md](ai/09-benchmark-baseline.md) | AI/maintainers | Internal benchmark note | keep-reference | Retained as an internal snapshot and cross-linked to the current public benchmark docs. |
| [docs/ai/10-competitive-plan-vs-postgres-mysql.md](ai/10-competitive-plan-vs-postgres-mysql.md) | AI/maintainers | Internal strategy snapshot | keep-reference | Explicitly marked as a strategy snapshot and cross-linked to the active backlog. |
| [docs/ai/11-technical-gap-matrix-vs-postgres.md](ai/11-technical-gap-matrix-vs-postgres.md) | AI/maintainers | Internal gap analysis | keep-reference | Refreshed to align with the current read-routing state and framed as strategic rather than the live sequence. |
| [hospitalapp/README.md](../hospitalapp/README.md) | Example-app users | Example app guide | keep-reference | Explicitly positioned as a supporting vertical example, not the primary ASQL onboarding path. |
| [hospitalapp/FRICTION_LOG.md](../hospitalapp/FRICTION_LOG.md) | Product/engineering | Evidence log | keep-reference | Keep as adoption evidence, not as canonical product guidance. |

## Recommended next cleanup sequence

1. Treat future changes as periodic documentation maintenance instead of broad cleanup.
2. Re-run a lightweight consistency pass whenever runtime posture, onboarding flow, or benchmark/commercial posture changes materially.
3. Continue routing any document pending deletion review through [docs/legacy/README.md](../legacy/README.md).
