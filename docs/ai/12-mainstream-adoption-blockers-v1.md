# Mainstream adoption blockers after the current compatibility wedge v1

Date: 2026-03-17

Purpose:
- identify the next 3 highest-value blocked or still-unvalidated adoption flows after the current validated wedge,
- classify each blocker by owning layer,
- recommend the next execution target for Epic AI.

Use with:
- [05-backlog.md](05-backlog.md)
- [../reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- [../reference/pgwire-driver-guidance-v1.md](../reference/pgwire-driver-guidance-v1.md)
- [../operations/pgwire-compatibility-test-pack-v1.md](../operations/pgwire-compatibility-test-pack-v1.md)

## Current validated wedge

ASQL already has direct evidence for:
- `psql` startup/introspection flows,
- DBeaver/DataGrip-style startup and schema browsing,
- `pgx` startup and end-to-end CRUD/query flows,
- the currently documented simple-query and extended-query subset,
- narrow `COPY`, cancellation, and compatibility-critical SQLSTATE behavior.

That means the next Epic AI step should not re-audit the existing wedge. It should target the next flows a serious evaluator is likely to try immediately after `pgx`, `psql`, or DBeaver succeed.

## Ranked next blockers

## 1. ORM-lite application path (`psycopg` / SQLAlchemy Core / GORM-like simple service)

Rank: 1

Why this is high value:
- this is the fastest path from “tool startup works” to “my actual app works”,
- it directly affects pilot conversion, not just interactive evaluation,
- it tests the real boundary between ASQL's pragmatic PostgreSQL subset and mainstream app assumptions.

Current blocker shape:
- partially blocked, partially undocumented, and currently high-risk.

Primary owner bucket:
- SQL surface

Secondary owner buckets:
- docs
- protocol/query-mode guidance

Why it is blocked today:
- [pgwire driver guidance](../reference/pgwire-driver-guidance-v1.md) explicitly marks ORMs and broad query builders as a known-risk path rather than a blessed path.
- [compatibility surface](../reference/postgres-compatibility-surface-v1.md) explicitly leaves several common ORM assumptions unsupported today, including:
  - bare `BEGIN` / `START TRANSACTION`,
  - `UPDATE ... RETURNING` / `DELETE ... RETURNING`,
  - arrays / `ANY(...)`,
  - broader PostgreSQL feature parity beyond the documented subset,
  - full PostgreSQL prepared-statement semantics beyond the current session-scoped path.

Likely first failure modes:
- ORM emits bare PostgreSQL transaction syntax,
- ORM assumes wider `RETURNING` coverage than ASQL documents,
- ORM emits array- or PostgreSQL-specific predicates,
- ORM relies on metadata or type assumptions outside the current shim.

Expected adoption impact if improved:
- very high; this is the most direct path from evaluation success to a real application pilot.

Recommended execution stance:
- do not chase broad ORM parity,
- instead validate one deliberately narrow ORM-lite path and document the exact safe subset.

## 2. `pgAdmin` startup + schema browsing path

Rank: 2

Why this is high value:
- it is a mainstream PostgreSQL GUI expectation,
- it is highly visible in evaluation loops,
- it is likely lower effort than a broad app-surface expansion.

Current blocker shape:
- unvalidated, with likely protocol/catalog fit but no regression-covered claim yet.

Primary owner bucket:
- protocol/catalog shim

Secondary owner buckets:
- docs
- release validation pack

Why it is blocked today:
- [pgwire driver guidance](../reference/pgwire-driver-guidance-v1.md) explicitly lists `pgAdmin` as “Unvalidated / caution”.
- the current validated GUI lane covers DBeaver/DataGrip startup flows, but not `pgAdmin`.
- the current compatibility pack and public evidence map do not list a regression-covered `pgAdmin` lane.

Likely first failure modes:
- additional startup/session probes not yet intercepted,
- slightly different metadata expectations around catalogs or object browsing,
- query-mode or connection-option differences relative to the already-covered GUI tools.

Expected adoption impact if improved:
- high visibility and relatively quick trust win for evaluators.

Recommended execution stance:
- validate `pgAdmin` as the first Epic AI implementation target after this audit,
- keep the scope narrow: startup, connection, schema browse, and one documented CRUD/query flow.

## 3. BI-lite read-only dashboards (`Grafana` / `Metabase`-style PostgreSQL datasource path)

Rank: 3

Why this is high value:
- read-only dashboard connectivity is a common “can we point existing tooling at it?” question,
- it exercises current-state `SELECT`, metadata, pagination, ordering, and aggregation without forcing full app parity,
- it can produce fast adoption wins if a narrow documented subset works.

Current blocker shape:
- unvalidated and likely partially blocked.

Primary owner bucket:
- protocol/catalog shim

Secondary owner buckets:
- SQL surface
- docs

Why it is blocked today:
- the current wedge validates `psql`, DBeaver/DataGrip, and `pgx`, but not BI tools.
- [compatibility surface](../reference/postgres-compatibility-surface-v1.md) still explicitly does not promise:
  - full PostgreSQL type-system parity,
  - broad catalog parity,
  - broader PostgreSQL SQL features outside the current grammar/planner support.
- BI tools often depend on metadata discovery, timestamps/casts, parameterized filters, and read-only query shapes that are close to the wedge but not yet regression-covered as a productized flow.

Likely first failure modes:
- metadata introspection differences,
- type/cast expectations around time values,
- generated SQL shapes just beyond the current documented subset,
- driver defaults that assume PostgreSQL semantics outside the wedge.

Expected adoption impact if improved:
- medium-high; especially useful for operator and analytics-oriented evaluation.

Recommended execution stance:
- validate one narrow read-only BI-lite path rather than promising generic BI compatibility.

## Classification summary

| Flow | Status now | Primary bucket | Secondary buckets | Expected impact |
|---|---|---|---|---|
| ORM-lite app path | Known-risk / partially blocked | SQL surface | Docs, query-mode guidance | Very high |
| `pgAdmin` | Unvalidated | Protocol/catalog shim | Docs, release validation | High |
| BI-lite dashboards | Unvalidated / likely partially blocked | Protocol/catalog shim | SQL surface, docs | Medium-high |

## Recommended next implementation target

Start with `pgAdmin`.

Reason:
- it is likely the smallest high-visibility win,
- it fits Epic AI's “mainstream tool/app wedge” without forcing broad SQL expansion first,
- it should produce a concrete new compatibility lane and a public docs win quickly.

Recommended sequencing after that:
1. `pgAdmin` startup + schema-browse validation lane,
2. one narrow ORM-lite happy path with explicit constraints,
3. one narrow BI-lite read-only datasource path.

## Explicit non-goal for this slice

Do not interpret this note as a plan for:
- full ORM compatibility,
- full PostgreSQL app parity,
- full BI-tool compatibility,
- or broad expansion of unsupported PostgreSQL semantics.

The goal is to choose the next adoption-moving wedge, not to reopen parity-for-parity work.
