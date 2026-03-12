# Adoption signals v1

This note shows how to use existing ASQL admin surfaces as adoption signals rather than only runtime-health signals.

## Why these signals matter

An onboarding team usually needs to answer questions like:

- are we overusing `CROSS DOMAIN`?
- are time-travel reads becoming a normal workflow or still an expert-only path?
- are aggregate versions growing in a way we can explain?

ASQL already exposes enough surface to begin answering those questions.

## Signal 1: cross-domain breadth

Use:

```sql
SELECT
  total_begins,
  total_cross_domain_begins,
  cross_domain_begin_avg_domains,
  cross_domain_begin_max_domains
FROM asql_admin.engine_stats;
```

Interpretation:

- high `total_cross_domain_begins` relative to `total_begins` suggests the model may be pushing too much workflow orchestration into engine-level atomicity,
- rising `cross_domain_begin_avg_domains` suggests transactions are spanning too many boundaries,
- rising `cross_domain_begin_max_domains` is a review trigger even if average remains stable.

Related metrics:

- `asql_engine_cross_domain_begins_total`
- `asql_engine_cross_domain_begin_domains_avg`
- `asql_engine_cross_domain_begin_domains_max`

## Signal 2: temporal-query usage

Use:

```sql
SELECT
  total_time_travel_queries,
  time_travel_latency_p50_ms,
  time_travel_latency_p95_ms,
  time_travel_latency_p99_ms
FROM asql_admin.engine_stats;
```

Interpretation:

- `total_time_travel_queries` shows whether historical inspection is actually being used,
- latency percentiles show whether the workflow remains operationally practical.

Related metric:

- `asql_engine_time_travel_queries_total`

## Signal 3: entity churn for one aggregate root

Use:

```sql
SELECT *
FROM asql_admin.entity_version_history
WHERE domain = 'recipe'
  AND entity = 'master_recipe_entity'
  AND root_pk = 'recipe-001';
```

Or summarize it:

```sql
SELECT COUNT(*) AS version_count
FROM asql_admin.entity_version_history
WHERE domain = 'recipe'
  AND entity = 'master_recipe_entity'
  AND root_pk = 'recipe-001';
```

Interpretation:

- a surprisingly high version count often means the aggregate boundary is too broad,
- a surprisingly low version count can mean the team expected aggregate semantics but is still modeling mostly at row level,
- abrupt version-shape changes after schema evolution are a migration review signal.

## Review cadence

During adoption, review these signals at least when:

- the first multi-domain workflow lands,
- the first entity-backed versioned references land,
- a schema evolution changes entity or versioned-reference semantics,
- a new team or service starts using ASQL.

## What these signals do not mean

Do not treat them as proof that business workflow belongs in ASQL.
They are adoption and modeling signals, not a reason to push domain semantics into the engine.