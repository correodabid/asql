# 01. Overview

## What ASQL is

ASQL is a deterministic SQL engine written in Go.

It combines:

- embedded-first operation,
- explicit domain isolation,
- append-only WAL as source of truth,
- time-travel and replay,
- entity versioning for aggregate-oriented workflows,
- optional distributed operation through the pgwire + Raft runtime.

## When ASQL is a good fit

ASQL is a strong fit when your application needs one or more of these:

- reproducible debugging and auditability,
- explicit boundaries between domains or modules,
- time-travel reads,
- aggregate/version-aware references,
- a local-first database with a clear operational model.

Typical examples:

- internal business platforms,
- healthcare or finance backends,
- edge/offline-first systems,
- systems where “what did the system know at commit X?” matters.

## What is different from a typical SQL database

### 1. Transactions are domain-scoped

ASQL does not hide boundaries.

You begin work with either:

- `BEGIN DOMAIN <name>`
- `BEGIN CROSS DOMAIN <a>, <b>`

### 2. The WAL is the canonical truth

Materialized state is derived from ordered WAL records.
That is why replay and historical reads are first-class concepts rather than side features.

### 3. Time-travel is part of the product surface

You can query past state by `LSN` or logical timestamp and inspect row/entity evolution with supported helpers.

### 4. Entity versioning exists above raw rows

ASQL still exposes rows and `LSN`s, but application-facing workflows can also use:

- entities,
- entity versions,
- versioned foreign key capture,
- aggregate-oriented debugging.

## Adoption mindset

The easiest way to succeed with ASQL is:

1. start single-node,
2. model domains explicitly,
3. use normal SQL first,
4. introduce time-travel and fixtures early,
5. only move into more advanced temporal or aggregate workflows when the app needs them.

## Next step

Continue with [02-install-and-run.md](02-install-and-run.md).
