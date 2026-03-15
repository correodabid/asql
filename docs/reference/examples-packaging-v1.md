# ASQL Examples Packaging v1

Date: 2026-03-15

## Purpose

This document explains how ASQL example assets are packaged and how teams should use them.

The goal is to make examples easier to discover without creating a parallel onboarding path.

Examples in ASQL should help users move deeper into the canonical product model:
- `cmd/asqld` as the runtime,
- pgwire as the main application-facing path,
- ASQL Studio as the desktop workflow,
- deterministic fixtures, temporal reads, and explicit domain boundaries as normal practice.

## Packaging rule

Examples are grouped by adoption moment, not by raw repository location.

Use this map:

### 1. First application integration

Use when a team wants the smallest application-facing pgwire example.

Primary references:
- [docs/getting-started/09-go-sdk-and-integration.md](../getting-started/09-go-sdk-and-integration.md)
- [docs/reference/cookbook-go-sdk.md](cookbook-go-sdk.md)

Expected shape:
- Go service using `pgx` or `pgxpool`
- explicit `BEGIN DOMAIN` / `COMMIT` flow
- ordinary reads and writes over pgwire
- temporal helper queries over normal SQL

### 2. Low-level admin / internal API reference

Use only when a team intentionally needs the lower-level gRPC/admin surface.

Reference asset:
- [../../examples/go-client/main.go](../../examples/go-client/main.go)

Positioning rule:
- this is a lower-level reference,
- it is not the default onboarding path,
- it should not be presented as the main way new teams integrate ASQL.

### 3. Deeper multi-domain adoption example

Use when a team has already read the core getting-started chapters and wants one example that combines domains, fixtures, history, entities, and versioned references.

Reference asset:
- [../../bankapp/README.md](../../bankapp/README.md)

What it is for:
- deeper adoption learning,
- explicit multi-domain transaction modeling,
- deterministic fixture-first workflows,
- temporal debugging and historical inspection.

What it is not:
- a vertical banking product template,
- a replacement for the getting-started path.

### 4. Compliance-heavy traceability example

Use when a team wants a deeper example where history, traceability, and temporal capture matter heavily.

Reference asset:
- [../../pharmaapp/README.md](../../pharmaapp/README.md)

What it is for:
- high-traceability modeling,
- temporal capture across domains,
- versioned references plus replay-safe inspection,
- learning where engine-owned concerns stop and app-owned compliance semantics begin.

What it is not:
- a pharma product,
- a signal that ASQL should absorb vertical workflow semantics.

### 5. Fixture-first adoption assets

Use when the team needs deterministic setup earlier than full service integration.

Primary references:
- [docs/getting-started/07-fixtures-and-seeding.md](../getting-started/07-fixtures-and-seeding.md)
- [../../bankapp/fixtures/banking-core-demo-v1.json](../../bankapp/fixtures/banking-core-demo-v1.json)
- [../../pharmaapp/fixtures/pharma-manufacturing-demo-v1.json](../../pharmaapp/fixtures/pharma-manufacturing-demo-v1.json)

## Recommended discovery order

Use this order on the docs site and in onboarding conversations:

1. getting started,
2. pgwire application integration,
3. fixture-first workflow,
4. one deeper reference app,
5. low-level admin/gRPC example only when intentionally needed.

## Positioning rules for all examples

### Rule 1 — canonical runtime first

Every example should keep the canonical runtime visible:
- start `cmd/asqld`,
- connect through pgwire unless the example is explicitly about lower-level admin APIs,
- mention Studio through `-pgwire-endpoint` when the workflow benefits from it.

### Rule 2 — no accidental PostgreSQL overclaim

Examples may use PostgreSQL-oriented tools and drivers through pgwire, but they must not imply full PostgreSQL parity.

### Rule 3 — examples support onboarding, they do not replace it

The primary onboarding path remains:
- [../getting-started/README.md](../getting-started/README.md)

Examples should extend that path, not fragment it.

### Rule 4 — reference apps are adoption tools, not product verticals

`bankapp` and `pharmaapp` exist to surface adoption and modeling lessons.
They should not drive ASQL toward vertical workflow semantics in the engine.

## Suggested docs/site presentation

When packaging examples publicly, present them as:
- **Quick integration** — pgwire + Go service
- **Fixture-first workflow** — deterministic scenario setup
- **Reference app: BankApp** — multi-domain / entities / history
- **Reference app: PharmaApp** — traceability-heavy / temporal capture
- **Advanced admin example** — lower-level gRPC/admin reference

## Bottom line

Good ASQL examples should reduce adoption friction while reinforcing the same product story everywhere:
- explicit domains,
- deterministic replay,
- temporal inspection,
- pgwire-first application integration,
- and a clear boundary between engine-owned and app-owned concerns.
