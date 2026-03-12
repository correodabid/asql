# Temporal modeling troubleshooting v1

This note is for adoption-time failures around:

- `VERSIONED FOREIGN KEY`,
- entity boundaries,
- row-head `LSN` vs entity-version capture,
- `FOR HISTORY`,
- `AS OF LSN`,
- and over-broad `CROSS DOMAIN` scope.

Use it when the SQL is valid but the model still feels wrong.

## Symptom: versioned reference write fails

Typical causes:

- the referenced row is not visible yet,
- the referenced root row exists but the expected entity does not,
- the application expects entity semantics but the referenced table is still row-based,
- the application expects row-head semantics but the referenced table is an entity root.

Check:

1. `SELECT resolve_reference('domain.table', 'pk')`
2. `SELECT row_lsn('domain.table', 'pk')`
3. `SELECT entity_version('domain', 'entity_name', 'pk')`

Interpretation:

- if `resolve_reference(...)` matches `row_lsn(...)`, you are using row-head capture,
- if `resolve_reference(...)` matches `entity_version(...)`, you are using entity-version capture.

Fix direction:

- keep the table row-based if row history is the clearest model,
- add or correct `CREATE ENTITY` only if the application truly reasons in aggregate revisions.

## Symptom: historical explanation is confusing even though writes succeed

Typical causes:

- the team cannot tell whether a reference stores an `LSN` or an entity version,
- `FOR HISTORY` is being used to explain an aggregate when row history alone is not enough,
- `AS OF LSN` reads are being run without first choosing the right historical token.

Recommended sequence:

1. inspect current state,
2. inspect `row_lsn(...)` or `entity_version(...)`,
3. inspect `FOR HISTORY`,
4. resolve the exact `LSN` to explain,
5. run `AS OF LSN` reads across every table that belongs in the explanation.

## Symptom: an entity exists, but the team still cannot explain what version means

Typical causes:

- `ROOT` was chosen for convenience rather than identity,
- `INCLUDES` pulled in tables that are only query-adjacent,
- the aggregate lifecycle is still not clear in application language.

Check:

- can the team explain the lifecycle in one sentence?
- do child tables really move with the same business revision?
- should downstream references preserve one aggregate revision or just one row mutation point?

Fix direction:

- simplify the entity,
- or remove it and stay row-based until the aggregate boundary becomes clear.

## Symptom: `CROSS DOMAIN` appears everywhere

Typical causes:

- domains were split too early,
- transaction scope is being used to represent workflow orchestration,
- UI adjacency is being mistaken for engine-level atomicity.

Check:

```sql
SELECT
  total_begins,
  total_cross_domain_begins,
  cross_domain_begin_avg_domains,
  cross_domain_begin_max_domains
FROM asql_admin.engine_stats;
```

Fix direction:

- re-check whether some steps should be app orchestration instead of one atomic engine write,
- reduce domain count per transaction before expanding it.

## Symptom: migration looks SQL-safe but feels historically risky

Typical causes:

- changing a `VERSIONED FOREIGN KEY` target,
- changing the column that stores the temporal token,
- changing entity `ROOT` or `INCLUDES`,
- dropping a temporal capture column and recreating it with a different meaning.

Check:

- whether the migration changes what `resolve_reference(...)` would capture,
- whether the migration changes aggregate boundaries,
- whether rollback restores historical meaning or only syntactic shape.

Fix direction:

- treat entity and versioned-reference changes as history-visible changes,
- require explicit preflight review instead of treating them as ordinary refactors.

## Symptom: too much temporal logic is leaking into app code

Typical causes:

- the team stayed entirely at raw SQL call sites,
- there is no small shared helper layer for history and snapshot reads,
- row and aggregate explanation patterns are mixed ad hoc.

Fix direction:

- keep the engine primitives explicit,
- but wrap repeatable patterns like:
  - `current -> history -> AS OF LSN -> explanation`,
  - `entity version -> entity_version_lsn -> snapshot read`,
  - `row_lsn -> history -> snapshot read`.

## Escalation rule

Escalate the issue as engine/product work when:

- the model is clear but the diagnostics remain opaque,
- the supported surface is too implicit for new teams,
- or repeated teams need the same helper/tooling pattern.

Do not escalate it as engine work when the real issue is:

- business semantics,
- workflow meaning,
- compliance vocabulary,
- or application-specific lifecycle design.
