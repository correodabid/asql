# ASQL Launch Narrative v1

Date: 2026-03-15
Status: public-positioning draft for `v1.0.0-rc1` through GA.

## Purpose

This document defines the short public story ASQL should tell across:
- website copy,
- docs landing pages,
- README/front door surfaces,
- demo scripts,
- technical launch post,
- release-candidate and GA announcement material.

The goal is not to maximize claims.
The goal is to make the product legible, credible, and differentiated.

## One-sentence positioning

ASQL is a deterministic SQL engine for teams that need explicit domain boundaries, replay-safe history, and operationally clear failover without giving up a practical pgwire application path.

## Short public description

ASQL is a deterministic SQL engine built in Go.
It combines:
- explicit domain isolation,
- append-only WAL as the source of truth,
- time-travel and historical inspection,
- deterministic replay,
- and optional clustered operation through pgwire + Raft.

It exposes a pragmatic PostgreSQL-compatible subset over pgwire for documented client and tool flows.
It is not a drop-in PostgreSQL replacement.

## The core problem statement

Most teams can get one or two of these at a time:
- simple local deployment,
- SQL ergonomics,
- explicit data boundaries,
- reproducible history,
- operationally understandable failover.

ASQL is for teams that need those together.

## The public value pillars

### 1. Deterministic replay
Same WAL input should produce the same state and the same query-visible results.

Public language:
- reproduce incidents exactly,
- inspect historical state without guessing,
- trust replay as an operational tool, not just a recovery mechanism.

### 2. Explicit domain isolation
ASQL makes boundaries visible.

Public language:
- domains are not cosmetic schema prefixes,
- cross-domain work is explicit,
- the database model helps teams reason about ownership and coupling.

### 3. Replay-safe temporal workflows
History is a normal workflow, not an afterthought.

Public language:
- `AS OF LSN`, `FOR HISTORY`, and temporal helpers are built-in debugging tools,
- no separate event-store project is required just to answer historical questions.

### 4. Operational clarity
ASQL should feel understandable under load, recovery, and failover.

Public language:
- append-only truth,
- clear release gates,
- visible leader/replica and failover state,
- operator-facing observability.

### 5. Practical adoption path
ASQL should not force proprietary clients.

Public language:
- pgwire-first application path,
- practical `pgx` / `psql` / mainstream tooling support inside the documented subset,
- single-node first, cluster when needed.

## What ASQL is not

Use this language explicitly when needed:
- not a drop-in PostgreSQL replacement,
- not a workflow engine,
- not a vertical banking/pharma/manufacturing product,
- not a feature-maximal SQL engine chasing parity for its own sake.

## Who ASQL is for

Best-fit users:
- teams that care about auditability and historical debugging,
- teams with explicit domain boundaries inside one product,
- offline/edge or controlled-environment deployments that benefit from deterministic replay,
- teams that want SQL ergonomics but need a more inspectable state model.

## Who should probably not start with ASQL

- teams that need broad PostgreSQL parity immediately,
- teams whose main requirement is analytical scale-out,
- teams that do not benefit from explicit boundaries or historical inspection.

## Message hierarchy by surface

### README / front door
Goal: explain what ASQL is in one screen.

Keep emphasizing:
- deterministic SQL engine,
- explicit domains,
- replay-safe history,
- pgwire subset,
- canonical `asqld` path.

### Docs landing
Goal: reduce adoption friction.

Keep emphasizing:
- canonical pgwire runtime,
- getting-started first,
- compatibility docs explicit,
- examples grouped by adoption moment.

### Studio page
Goal: show operator and developer delight.

Keep emphasizing:
- temporal exploration,
- cluster and failover visibility,
- schema and SQL workflows,
- not just visual polish.

### Launch post / announcement
Goal: explain why ASQL exists.

Recommended structure:
1. the problem with hidden state and invisible boundaries,
2. the ASQL model: domains + WAL truth + temporal workflows,
3. why determinism matters operationally,
4. pgwire-first adoption path,
5. what ASQL supports today and what it deliberately does not.

## Claim discipline

Public claims should always follow these rules:
- prefer precise over broad,
- distinguish supported subset from parity,
- distinguish benchmark evidence from production SLOs,
- distinguish engine-owned capabilities from app-owned workflow semantics.

## Suggested hero-copy variants

### Variant A
The SQL engine that remembers everything.

Deterministic replay, explicit domains, and replay-safe history on a practical pgwire path.

### Variant B
A deterministic SQL engine for systems you need to explain.

Trace boundaries explicitly. Query historical state directly. Reproduce behavior from WAL, not guesswork.

### Variant C
SQL with explicit boundaries and replay-safe history.

Start with a single node. Scale to a Raft-backed cluster. Keep the same mental model.

## Bottom line

ASQL should launch as the database for teams who want state they can reason about:
- explicit boundaries,
- deterministic replay,
- first-class historical inspection,
- and an operational story that stays understandable from local runtime to clustered operation.
