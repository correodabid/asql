# ASQL Studio

`asqlstudio/` is the canonical Studio product folder.

Source of truth:

- Go backend sources live in this folder.
- Frontend source lives under `webapp/`.
- `webapp/src/` is the canonical UI source.

Generated artifacts:

- `app/web/` is a generated embed target produced by `npm run build` from `webapp/`.
- Do not edit files under `app/web/` manually.
- Local frontend dependencies under `webapp/node_modules/` are not canonical source.

Common workflows:

- Run Studio: `go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql`
- Run Studio with operator/admin surfaces: `go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -admin-endpoints 127.0.0.1:9090 -admin-auth-token <token> -data-dir .asql`
- Build frontend assets: `cd ./asqlstudio/webapp && npm run build`

UX note:

- Studio opens on a guided `Start Here` overview so first-run users can move through domain selection, fixtures/schema, first query, and temporal exploration without having to infer the happy path from raw tabs alone.
- Studio now includes a runtime connection manager in the title bar and status bar so operators can retarget pgwire/admin endpoints without relaunching the desktop app, keep a recent-connection list with one-click reconnect for quick reuse of previous targets, and save or rename named connection profiles for stable environments like local dev, demo clusters, or shared staging nodes.
- Studio EXPLAIN workflows surface a planner verdict summary plus runtime access-plan detail, including indexed predicates, residual predicates, evaluated candidates, heuristic-pruned candidates, operator-facing plan suggestions, and plan-tree highlights that map indexed vs residual predicate work back onto the shape of the query, including inline subexpression highlighting for compound filters.
- The query toolbar includes a per-tab `EXPLAIN` toggle so operators can inspect plans without rewriting the SQL text in the editor.
- The Ops area includes a `Security` panel for durable principal bootstrap, user/role creation, grants and revocations, password rotation, principal enable/disable operations, effective role visibility, and safe deletion of disabled unreferenced principals over the admin HTTP surface.