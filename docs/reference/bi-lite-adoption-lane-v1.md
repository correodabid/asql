# BI-lite adoption lane v1

This note defines the narrow read-only dashboard/datasource path ASQL
currently supports for PostgreSQL-oriented BI evaluation.

Use it together with:

- [pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [orm-lite-adoption-lane-v1.md](orm-lite-adoption-lane-v1.md)

## Scope

This is not a claim of broad BI-tool compatibility.

It is a claim that a narrow read-only datasource path can work today for a
Grafana-style or Metabase-style evaluation when the tool is used in explicit
SQL mode and the queries stay inside the documented ASQL subset.

Recommended use cases:

- dashboard evaluation with custom SQL,
- read-only operational summaries,
- small aggregate or table panels over current-state data,
- exploratory validation that an existing SQL dashboard can be translated into the ASQL subset.

## Recommended connection shape

Start with:

```text
postgres://asql@127.0.0.1:5433/asql?sslmode=disable&default_query_exec_mode=simple_protocol
```

Use `sslmode=prefer` when the client expects PostgreSQL TLS negotiation but can
fall back to plaintext.

Why this is the default:

- it is the lowest-surprise pgwire path,
- it keeps dashboard SQL easy to inspect,
- it avoids broad prepared-statement/query-builder assumptions during the first evaluation pass.

## Regression-covered happy path

The current lane is regression-covered in
[internal/server/pgwire/server_test.go](../../internal/server/pgwire/server_test.go#L750-L886)
via `TestPGWireBILiteReadOnlyPath`.

That lane proves:

- pgwire connection with `simple_protocol`,
- basic datasource/session identity via `current_database()`,
- schema-browse metadata through `information_schema.tables` and `information_schema.columns`,
- parameterized read-only filtering with `ORDER BY` and `LIMIT`,
- grouped read-only aggregates with `COUNT(*)`, `GROUP BY`, and `ORDER BY`.

## What this lane is good for

- table-format dashboards with explicit SQL,
- simple grouped summaries,
- filtered current-state read panels,
- lightweight operational dashboards over ASQL-managed application data.

## Translation rules and operating rules

| Desired BI behavior | Current ASQL path |
|---|---|
| connect a PostgreSQL-style datasource | Use pgwire with `sslmode=disable` or `sslmode=prefer` and `simple_protocol` first. |
| browse visible tables/columns | Rely on the current documented `information_schema.tables` / `information_schema.columns` subset. |
| build table-style panels | Prefer explicit SQL in code/custom-SQL mode. |
| filtered read panels | Use parameterized scalar predicates and the current supported `ORDER BY` / `LIMIT` subset. |
| grouped summary panels | Use current supported `COUNT`, `SUM`, `AVG`, `GROUP BY`, and `HAVING` shapes that are already in the SQL subset. |

## Known unsupported or unclaimed edges

Do not treat the following as part of this BI-lite claim:

- generic dashboard-builder/query-builder compatibility,
- time-macro expansion layers such as Grafana-specific macro rewriting,
- builder-mode table auto-discovery queries that rely on PostgreSQL functions or catalog behavior beyond the documented shim subset,
- full PostgreSQL date/time type parity for time-series-specific dashboards,
- arbitrary datasource autocomplete flows,
- write-capable BI integrations.

## Recommended rollout order

1. prove one read-only panel with explicit SQL,
2. prove one grouped summary query,
3. validate only the exact metadata discovery and datasource settings actually used by the target tool,
4. document the unsupported builder/time-series edges explicitly in the team playbook.

If a custom-SQL read-only panel works but a higher-level builder path fails,
classify that as tool-surface validation work rather than as evidence that ASQL
needs broad PostgreSQL parity.