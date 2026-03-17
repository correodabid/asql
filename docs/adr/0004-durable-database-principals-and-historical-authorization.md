# ADR 0004: Make database principals durable engine state and authorize historical access explicitly

- Status: Accepted
- Date: 2026-03-17
- Decision drivers:
  - database users, roles, and grants must survive restart, replay, and clustered replication
  - historical access must remain auditable without pretending newly created principals existed in the past
  - transport/admin tokens should stay deployment/operator controls, not the long-term database security model
  - ASQL must preserve deterministic replay and explicit temporal semantics

## Context

Today ASQL exposes transport-level authentication surfaces, but not a real database-principal model.

Current posture:

- pgwire accepts one shared configured password token when [internal/server/pgwire/server.go](../../internal/server/pgwire/server.go) is started with `AuthToken`.
- admin HTTP and gRPC flows are protected by bearer tokens derived from process config.
- compatibility shims such as `has_schema_privilege(...)` and `pg_roles` currently exist for interoperability, not for real authorization semantics.

That is sufficient for a narrow single-token compatibility wedge, but it is not sufficient for a multi-user database product.

The missing capability became especially visible around time-travel and history:

- ASQL can read old state with `AS OF LSN`, `AS OF TIMESTAMP`, and `FOR HISTORY`.
- there was no first-class rule saying who is allowed to do that
- there was also no durable identity/grant state that could be replayed alongside the rest of the engine state

If users/roles/grants lived only in process config or an external side store, replay could reconstruct data state without reconstructing the security state under which the system is supposed to operate.
That would break restart correctness, weaken auditability, and create drift between nodes.

## Decision

ASQL will treat database principals and grants as durable engine-owned metadata.

Concretely:

1. users, roles, memberships, and grants will be represented in durable state and reconstructed through WAL replay
2. passwords will never be stored in cleartext in durable records; only a password-hash or secret-reference form may be persisted
3. pgwire database authentication will authenticate against stored principals rather than a permanent fixed logical user
4. historical access will require an explicit privilege separate from ordinary `SELECT`
5. historical authorization will be evaluated against the **current** principal/grant state, even when the data snapshot being queried is old
6. audit output must make both facts visible:
   - which historical point was queried
   - which current grant state authorized the read

## Why current-state authorization for old data

ASQL time-travel semantics and authorization semantics solve different questions:

- time-travel asks: “what did the data look like at LSN X?”
- authorization asks: “is this principal allowed to request that view now?”

Those should not be conflated.

Therefore, if a user is created today and is granted historical-read access today, that user may read rows from old `LSN`s without ASQL inventing that the user existed back then.

This keeps the model coherent:

- the data snapshot is historical
- the authorization decision is current
- the audit trail shows the explicit grant sequence

ASQL will not backdate principal existence or role membership merely because a query targets old data.

## Target model

### 1. Durable principal catalog

ASQL will introduce durable principal metadata for:

- `USER`
- `ROLE`
- role membership
- principal state such as `enabled`, `disabled`, or `locked`
- privilege grants and revokes

This metadata is part of engine state, not only bootstrap config.

### 2. Explicit temporal privilege

ASQL will introduce a first-class temporal-read privilege, initially modeled as `SELECT_HISTORY` or an equivalent engine constant.

The intent is:

- ordinary `SELECT` is not enough for time-travel/history access
- `AS OF LSN`, `AS OF TIMESTAMP`, and `FOR HISTORY` are privileged operations
- operators can grant current reads without automatically granting historical visibility

### 3. Bootstrap semantics

ASQL still needs a safe way to create the first admin principal.

The bootstrap rule will be:

- a dedicated bootstrap path is allowed only when the durable principal catalog is empty
- after the first admin principal exists, steady-state user/role management must go through the durable model

This preserves operability without making long-term identity management configuration-only.

### 4. Transport tokens remain separate

Process-config tokens still have a role, but a narrower one:

- cluster peer authentication
- admin API/operator bootstrap protection
- deployment-time operator controls

They are not the same thing as database principals and should not be presented as such.

## Initial execution slice

The first vertical slice should deliver:

1. durable principal catalog state with replay-safe WAL representation
2. bootstrap of the first admin principal when the catalog is empty
3. pgwire authentication against stored principals
4. one explicit temporal privilege checked on history/time-travel paths
5. minimal admin/CLI management for principal creation and granting temporal access

Studio should come after the engine semantics and CLI/admin flows are stable.

## Consequences

### Positive

- restart and replay reproduce security state, not only data state
- clustered nodes can converge on the same effective permission model
- historical access becomes explicit and auditable
- ASQL can explain a clear difference between transport tokens and database identities

### Negative / costs

- replay, snapshots, and auth handshakes must all be extended
- compatibility shims that currently always succeed will need either real enforcement or narrower claims
- admin and CLI surfaces need new secure workflows for bootstrap and grant inspection

### Preserved

- ASQL remains deterministic-first
- historical reads remain based on old data snapshots
- the system still supports operator-controlled bootstrap flows

## Alternatives considered

### A. Keep users/roles outside the WAL

Rejected.

That would let state replay reconstruct one world for data and another for security.
It also makes clustered convergence and auditability weaker.

### B. Treat historical reads as ordinary `SELECT`

Rejected.

Historical access is materially more sensitive and should not be an accidental byproduct of read access.

### C. Evaluate authorization using historical grant state at the target `LSN`

Rejected for the first model.

That would make authorization semantics much harder to reason about operationally and would blur the line between “what data existed then” and “what policy exists now”.
It may be interesting for specialized audit modes later, but it is not the default ASQL security model.

## Follow-on work

- implement the `AG-1` and `AH-1` backlog slices in [docs/ai/05-backlog.md](../ai/05-backlog.md#L647-L711)
- update compatibility docs so database-principal support and remaining unsupported PostgreSQL role-management statements are explicit
- add tests covering bootstrap, replay, disabled principals, explicit temporal grants, and denied history access