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
- Build frontend assets: `cd ./asqlstudio/webapp && npm run build`

UX note:

- Studio opens on a guided `Start Here` overview so first-run users can move through domain selection, fixtures/schema, first query, and temporal exploration without having to infer the happy path from raw tabs alone.
- Studio EXPLAIN workflows surface a planner verdict summary plus runtime access-plan detail, including indexed predicates, residual predicates, evaluated candidates, heuristic-pruned candidates, and operator-facing plan suggestions, so query-path rationale is visible without dropping to engine internals.