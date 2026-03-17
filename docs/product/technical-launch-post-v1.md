# ASQL Technical Launch Post v1

Date: 2026-03-17
Status: selected long-form launch post for `v1.0.0-rc1` through GA.
Audience: engineering readers, early adopters, and technical evaluators.

## Title

ASQL: a deterministic SQL engine for systems you need to explain

## Post

Most databases make you choose one or two of the following:
- easy local deployment,
- familiar SQL access,
- explicit data boundaries,
- replay-safe history,
- operationally understandable failover.

ASQL exists because some systems need all of them at once.

ASQL is a deterministic SQL engine built in Go.
It combines:
- explicit domain isolation,
- append-only WAL as the source of truth,
- time-travel and historical inspection,
- deterministic replay,
- and optional clustered operation through pgwire + Raft.

It exposes a pragmatic PostgreSQL-compatible subset over pgwire for documented client and tool flows.
It is not a drop-in PostgreSQL replacement.

## The problem: hidden state is expensive

In many production systems, the painful part is not only storing data.
It is explaining state after something went wrong.

Teams run into the same questions repeatedly:
- What exactly changed?
- What did the system believe at that moment?
- Which boundaries were crossed in one unit of work?
- Can we replay the same input and get the same answer?
- Which node is safe to trust after failover?

Traditional stacks often answer those questions with a patchwork of:
- logs,
- ad hoc audit tables,
- event-stream side systems,
- application conventions,
- and operational guesswork.

ASQL is designed to make those questions first-class.

## The ASQL model

### 1. Domains are explicit

Every transaction declares its scope.

That means boundaries are visible in the data model instead of being hidden in repository code or application folklore.
Single-domain and cross-domain work are both explicit.

This matters because many bugs are really boundary bugs:
- data that should not have been coupled,
- writes that should not have been atomic together,
- or ownership lines that were never modeled clearly.

### 2. The WAL is the source of truth

ASQL treats the append-only WAL as canonical.
Materialized state is derived.

That keeps the operational story simpler:
- recovery is explainable,
- historical state has a stable foundation,
- and replay is a normal capability rather than an afterthought.

### 3. History is queryable

ASQL includes replay-safe historical workflows as part of the normal SQL surface.

That includes:
- `AS OF LSN` reads,
- `FOR HISTORY`,
- and temporal helper functions for inspecting the current and historical head of rows and entities.

The point is not novelty.
The point is to make historical debugging part of ordinary work.

### 4. Determinism is a product constraint

The same WAL input should produce the same state and the same query-visible results.

That principle shapes the engine design:
- no hidden dependence on wall-clock behavior in core execution paths,
- deterministic ordering where it matters,
- replay and failover behavior that can be tested as equivalence rather than hand-waved as "close enough".

### 5. pgwire is the practical application path

ASQL is not trying to force a proprietary client model.

The canonical runtime path is:
- `cmd/asqld` for the server,
- pgwire for application-facing access,
- ASQL Studio connected through `-pgwire-endpoint`.

That gives teams a practical adoption path through documented `pgx`, `psql`, and mainstream tool flows inside a clearly bounded compatibility subset.

## What makes ASQL different

There are many databases with good SQL support.
There are databases with strong replication stories.
There are systems with audit logs or event streams.

ASQL is aimed at the overlap where teams need:
- SQL ergonomics,
- explicit boundaries,
- historical explainability,
- deterministic replay,
- and an operational model they can reason about from local runtime to clustered operation.

That is the wedge.
Not "better than PostgreSQL at everything".
Not maximum feature count.

## What ASQL is not

ASQL is not:
- a drop-in PostgreSQL replacement,
- a workflow engine,
- a vertical banking/pharma/manufacturing product,
- or a parity-driven SQL project that expands surface area without clear operational value.

That boundary matters.
ASQL should stay compact and sharp in the niche where it is strongest.

## Who should care

ASQL is a strong fit for teams that:
- care about auditability and historical debugging,
- have explicit domain boundaries inside one product,
- want SQL and pgwire ergonomics without giving up explainability,
- benefit from deterministic replay in controlled, edge, or compliance-heavy environments.

It is a weaker fit for teams that:
- need broad PostgreSQL parity immediately,
- mainly optimize for analytical scale-out,
- or do not benefit from explicit historical/state reasoning.

## What we are freezing for launch

The next phase for ASQL is not feature sprawl.
It is trust.

That means:
- explicit GA compatibility contracts,
- release gates for replay, recovery, upgrade, and compatibility,
- performance guardrails for the main write paths,
- a canonical docs and examples surface,
- and operator-facing clarity around temporal workflows, lag, and failover.

In other words: ASQL should launch as a system that is understandable and credible, not just interesting.

## Evaluate ASQL the intended way

The shortest safe evaluation path is:

1. start the canonical server runtime with `cmd/asqld`,
2. connect through pgwire,
3. model one explicit domain,
4. try historical reads and fixtures early,
5. inspect compatibility, benchmark, and production docs before widening claims,
6. then move into the deeper examples and operator workflows.

The product surfaces for that path are already organized around:
- getting started,
- pgwire-first integration,
- fixture-first adoption,
- deeper reference apps,
- compatibility docs,
- benchmark evidence,
- and release/production guidance.

## Closing

ASQL is for teams who want state they can explain.

If your system needs:
- explicit boundaries,
- replay-safe history,
- deterministic behavior,
- and a practical path from single node to clustered operation,
then ASQL is worth evaluating.

If what you need is broad PostgreSQL parity above all else, it probably is not.

That clarity is intentional.

## Short announcement snippets

### Social / short post
ASQL is a deterministic SQL engine built in Go: explicit domains, replay-safe history, `AS OF LSN`, deterministic replay, and a practical pgwire path from single node to Raft-backed cluster.

### Release blurb
ASQL launches as a deterministic SQL engine for teams that need explicit data boundaries, historical explainability, and operationally understandable failover without giving up practical SQL tooling.
