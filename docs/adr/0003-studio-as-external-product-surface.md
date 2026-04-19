# ADR 0003: Treat ASQL Studio as an external product surface, not an engine-internal app

- Status: Superseded by [ADR 0004](0004-studio-lives-in-a-separate-repository.md)
- Date: 2026-03-12

> **2026-04-19 update**: Alternative C of this ADR ("Extract Studio into a
> separate repository") was revisited and adopted. ASQL Studio now lives
> at [github.com/correodabid/asqlstudio](https://github.com/correodabid/asqlstudio).
> The decoupling work described below (public `pkg/adminapi`, `pkg/fixtures`,
> `pkg/servertest`) was completed first and made the extraction mechanical.
> See ADR 0004 for the full rationale.
- Decision drivers:
  - reduce coupling between Studio and engine internals
  - make ASQL Studio a clearer product sibling of `asqld` and `asqlctl`
  - preserve explicit public boundaries for admin, fixture, and temporal workflows
  - avoid letting folder layout hide architectural coupling

## Context

ASQL Studio originally lived under [cmd/asqlstudio](../../cmd/asqlstudio), which made it look like a thin binary entrypoint.
In practice, it is a substantial product surface with its own Wails app, frontend, schema tooling, diagnostics, and engine-facing client logic.

More importantly, Studio currently depends on engine internals rather than only on stable public surfaces.

Historical examples:

- Studio imports `internal/server/httpapi` contracts in [cmd/asqlstudio/engine_client.go](../../cmd/asqlstudio/engine_client.go), [cmd/asqlstudio/schema_apply.go](../../cmd/asqlstudio/schema_apply.go), and [cmd/asqlstudio/schema_introspection.go](../../cmd/asqlstudio/schema_introspection.go).
- Studio imports `internal/fixtures` in [cmd/asqlstudio/app.go](../../cmd/asqlstudio/app.go) and [cmd/asqlstudio/engine_client.go](../../cmd/asqlstudio/engine_client.go).
- Studio imports `internal/engine/executor` directly in [cmd/asqlstudio/app.go](../../cmd/asqlstudio/app.go) for backup/restore and storage inspection flows.

This creates two kinds of confusion:

1. **location confusion**: Studio appears to be a simple command, even though it is a product surface.
2. **boundary confusion**: Studio appears to be an engine client, but still reaches into engine internals.

Moving Studio out of `cmd` is desirable, but location alone does not solve the architectural problem.

## Decision

ASQL Studio will be treated as a product sibling that consumes stable public engine surfaces.

Concretely:

1. Studio implementation code should move to a root-level [asqlstudio](../../asqlstudio) folder.
2. `cmd/asqlstudio` should be reduced to a thin wrapper during transition, or removed if the root-level app becomes the canonical entrypoint.
3. Studio must stop importing `internal/engine/*` and `internal/server/*` packages.
4. Any contracts Studio needs from the engine must move to stable public packages or public admin APIs.
5. Fixture workflows consumed by Studio should move to a public product-facing package, not remain `internal`.

## Target architecture

### 1. Repository shape

Target direction:

- [asqlstudio](../../asqlstudio)
  - Wails app backend
  - frontend source
  - schema tooling
  - Studio-specific client/adapters
- [cmd/asqld](../../cmd/asqld)
  - server bootstrap
- [cmd/asqlctl](../../cmd/asqlctl)
  - CLI bootstrap

Implemented state:

- [asqlstudio](../../asqlstudio) is the canonical Studio product folder.
- `cmd/asqlstudio` has been removed.

The `cmd` directory should communicate “how to launch binaries,” not “where full applications live.”

### 2. Public boundary direction

Studio should talk to the engine through:

- pgwire for SQL, temporal helpers, and interactive queries,
- public admin HTTP/gRPC surfaces for operational and administrative flows,
- stable public Go packages only where sharing code is justified.

Studio should not import:

- `internal/engine/*`,
- `internal/server/*`,
- or other engine-private implementation packages.

### 3. Shared package extraction

The following capabilities should move out of `internal` if Studio needs them:

- admin request/response contracts currently under `internal/server/httpapi`,
- fixture load/validate/export contracts and helpers currently under `internal/fixtures`.

Target direction:

- `pkg/adminapi` or `sdk/adminapi`
- `pkg/fixtures`

Exact package names may evolve, but the boundary rule is part of the decision.

### 4. Remove direct executor use from Studio

Studio currently uses `internal/engine/executor` for:

- base backup creation,
- backup manifest inspection,
- backup verification,
- restore-to-lsn and restore-to-timestamp,
- snapshot catalog inspection,
- WAL retention inspection.

These capabilities should be exposed through public admin surfaces rather than direct imports.

If the engine supports a capability operationally, Studio should consume it the same way another external tool would.

## Refactor plan by slices

### Slice 0 — freeze boundary and target layout

Goal:

- confirm Studio as a sibling product,
- confirm that “move folder” is not the same as “decouple architecture.”

Deliverables:

- this ADR,
- backlog slices,
- agreed target shape for folders and package boundaries.

### Slice 1 — extract public contracts first

Goal:

- remove Studio dependency on `internal/server/httpapi` and `internal/fixtures`.

Expected work:

- create a public contracts package for admin DTOs,
- create a public fixtures package for load/validate/export workflows,
- update Studio imports to use the public packages.

Success condition:

- Studio no longer imports `internal/server/httpapi`,
- Studio no longer imports `internal/fixtures`.

### Slice 2 — replace direct executor calls with public admin surface

Goal:

- remove Studio dependency on `internal/engine/executor`.

Expected work:

- expose the required backup/restore and storage-inspection operations through public admin APIs,
- switch Studio handlers to those APIs.

Success condition:

- Studio no longer imports `internal/engine/executor`.

### Slice 3 — move Studio implementation to root-level `asqlstudio`

Goal:

- align folder structure with product structure.

First-cut file moves:

- [cmd/asqlstudio/app.go](../../cmd/asqlstudio/app.go) -> `asqlstudio/app.go`
- [cmd/asqlstudio/engine_client.go](../../cmd/asqlstudio/engine_client.go) -> `asqlstudio/engine_client.go`
- [cmd/asqlstudio/main.go](../../cmd/asqlstudio/main.go) -> `asqlstudio/main.go` or thin wrapper retained in `cmd/asqlstudio/main.go`
- [cmd/asqlstudio/schema_apply.go](../../cmd/asqlstudio/schema_apply.go) -> `asqlstudio/schema_apply.go`
- [cmd/asqlstudio/schema_diff.go](../../cmd/asqlstudio/schema_diff.go) -> `asqlstudio/schema_diff.go`
- [cmd/asqlstudio/schema_designer.go](../../cmd/asqlstudio/schema_designer.go) -> `asqlstudio/schema_designer.go`
- [cmd/asqlstudio/schema_introspection.go](../../cmd/asqlstudio/schema_introspection.go) -> `asqlstudio/schema_introspection.go`
- [cmd/asqlstudio/read_routing.go](../../cmd/asqlstudio/read_routing.go) -> `asqlstudio/read_routing.go`
- [cmd/asqlstudio/read_routing_metrics.go](../../cmd/asqlstudio/read_routing_metrics.go) -> `asqlstudio/read_routing_metrics.go`
- tests alongside each moved file
- [cmd/asqlstudio/wails.json](../../cmd/asqlstudio/wails.json) -> `asqlstudio/wails.json`
- [cmd/asqlstudio/webapp](../../cmd/asqlstudio/webapp) -> `asqlstudio/webapp`
- generated web assets aligned with the new location

Operational updates required:

- workspace tasks that currently point at `cmd/asqlstudio/webapp`,
- Wails embed paths,
- CI/build packaging paths,
- docs that describe Studio startup/build location.

### Slice 4 — harden the boundary

Goal:

- prevent architectural regression.

Expected work:

- add checks/tests that fail if Studio imports engine-internal packages,
- document Studio as an external consumer of public engine surfaces,
- stop tracking generated Studio build outputs if they are not source assets.

## Consequences

### Positive

- clearer product structure
- clearer engine-vs-tool boundary
- easier future extraction of Studio into a sibling repo if ever needed
- better discipline around public admin and fixture surfaces
- reduced risk that tooling starts depending on private engine mechanics

### Negative / costs

- requires contract extraction work before the folder move pays off
- may force new admin endpoints for currently internal operations
- Wails/build packaging paths and tasks will need adjustment
- short-term churn across tests, docs, and build scripts

### Neutral / preserved

- Studio can remain in the same repository
- Studio still remains a first-class product surface
- engine determinism and admin capabilities remain unchanged in principle

## Alternatives considered

### Alternative A: Keep Studio in `cmd` and change nothing else

Rejected.

This preserves both the location confusion and the internal-package coupling.

### Alternative B: Move Studio to the root but keep importing `internal/*`

Rejected.

This improves appearance, but not architecture.

### Alternative C: Extract Studio into a separate repository immediately

Rejected for now.

The right first move is to establish clean public boundaries inside the current repository.

## Acceptance signals

This ADR is successful when:

- Studio lives under a root-level [asqlstudio](../../asqlstudio) product folder,
- `cmd/asqlstudio` is only a thin launcher or disappears,
- Studio imports no `internal/engine/*` packages,
- Studio imports no `internal/server/*` packages,
- required admin and fixture capabilities are available through public surfaces,
- build/docs/tasks reflect the new canonical Studio location.

## Related documents

- [docs/ai/05-backlog.md](../ai/05-backlog.md)
- [docs/adr/0002-generalist-engine-boundary-and-adoption-surface.md](0002-generalist-engine-boundary-and-adoption-surface.md)