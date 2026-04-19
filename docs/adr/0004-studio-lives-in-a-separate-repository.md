# ADR 0004: ASQL Studio lives in a separate repository

- Status: Implemented
- Date: 2026-04-19
- Supersedes: [ADR 0003](0003-studio-as-external-product-surface.md) alternative C

## Context

[ADR 0003](0003-studio-as-external-product-surface.md) rejected Alternative C
("extract Studio into a separate repository immediately") in favor of first
establishing clean public boundaries inside the same repo. That decoupling
work is now complete: Studio no longer imports any `internal/*` packages,
and every engine capability Studio consumes lives under a stable `pkg/*`
surface.

With the boundary already enforced, the remaining argument for keeping
Studio in the monorepo was release cadence convenience. Against that, the
separation benefits dominate:

- **Distinct stacks**: the engine is Go only; Studio is Go + Wails + React +
  TypeScript. CI, dependency upgrades, and repository conventions diverge.
- **Distinct audiences**: operators/developers integrating ASQL vs
  end-users launching a desktop app. Their issues, release notes, and
  documentation shouldn't share a tracker.
- **Distinct release cadences**: UI iterates faster than the engine. A
  single tagged version forces either over-shipping the engine or
  under-shipping Studio.
- **Distinct binary outputs**: the engine ships as a server binary; Studio
  ships as a Wails desktop bundle (DMG/MSI/AppImage). Build pipelines
  benefit from not sharing Makefiles or CI matrices.

Industry precedent (Postgres/pgAdmin, MySQL/Workbench, Mongo/Compass,
Redis/RedisInsight) is to keep the engine and its primary UI in separate
repos. Supabase is the notable counter-example; its monorepo bet depends
on treating the engine and UI as one product — a framing ASQL does not
adopt.

## Decision

ASQL Studio moves to [github.com/correodabid/asqlstudio](https://github.com/correodabid/asqlstudio)
as a standalone Go module. The monorepo retains the engine, server,
client CLI, SDK (if it persists), and public `pkg/*` contracts.

Mechanics:

1. `asqlstudio/` is removed from the `asql` monorepo.
2. The extracted repository declares `module github.com/correodabid/asqlstudio`
   and depends on `github.com/correodabid/asql` via the `pkg/*` surface
   only.
3. Until the engine repo is published, the Studio `go.mod` uses a
   `replace` directive pointing at a sibling checkout (`../asql`). When
   the engine module is public, the replace is dropped and a tagged
   version is pinned.
4. All engine-dependent tests that required in-process pgwire use the
   new `github.com/correodabid/asql/pkg/servertest` helper.

## Target architecture

```
github.com/correodabid/asql
├── cmd/            # asqld, asqlctl
├── internal/       # engine, server, cluster, storage
├── pkg/
│   ├── adminapi    # admin HTTP request/response types
│   ├── fixtures    # fixture format + Apply/Export
│   └── servertest  # ephemeral pgwire server for tests
└── docs/

github.com/correodabid/asqlstudio
├── app/            # Wails backend (Go)
├── webapp/         # Frontend (React/TypeScript)
└── main.go
```

Studio imports only `github.com/correodabid/asql/pkg/*`. The monorepo
imports nothing from Studio.

## Consequences

### Positive

- Engine and Studio release on independent cadences.
- Issue trackers, docs, and contribution guides specialize to their
  audience.
- Studio's Wails/React build complexity stops polluting engine CI.
- `pkg/*` becomes a real public contract, not a convenience facade —
  every change breaks a downstream module and has to be justified.

### Negative / costs

- Changes that affect both sides require two PRs, one per repo.
- The `replace` directive in Studio's go.mod is a friction point for
  contributors until the engine is published as a Go module.
- Studio's `ValidateDryRun` feature no longer validates against an
  in-process engine (it was removed; Studio now relies on apply-time
  errors from pgwire). A future ADR may reintroduce dry-run as a
  server-side RPC.

### Neutral / preserved

- Language, build tools, and deployment targets are unchanged for both
  sides.
- The engine's determinism, audit, and time-travel capabilities remain
  internal to the engine module.

## Related documents

- [ADR 0003: Treat ASQL Studio as an external product surface](0003-studio-as-external-product-surface.md)
