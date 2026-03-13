# Schema evolution checklist for entities and versioned references v1

Use this checklist before approving schema changes that touch:

- `CREATE ENTITY` boundaries,
- `VERSIONED FOREIGN KEY`,
- temporal token columns such as `*_version` or `*_lsn`,
- or tables that are already part of temporal explanation workflows.

## 1. Classify the change

Ask which class the change belongs to:

- additive column/index change,
- destructive column/table change,
- versioned-reference change,
- entity boundary change,
- replay/history-visible semantic change.

If the answer includes the last three, do not treat it as routine schema maintenance.

## 2. Review temporal meaning, not only SQL shape

Before approval, answer explicitly:

- does this change alter what token a downstream reference captures?
- does it move a table into or out of an entity lifecycle?
- does it change whether downstream code reasons in row-head `LSN` or entity version?
- does it change how `resolve_reference(...)` should be interpreted?

## 3. Review rollback meaning

A rollback is only acceptable if it restores more than syntax.
It should restore the historical meaning visible to applications and operators.

Ask:

- after rollback, would `resolve_reference(...)` behave the same way as before?
- after rollback, would `entity_version(...)` still describe the same aggregate boundary?
- would replay and historical explanation still make sense to the team reading incidents later?

## 4. Treat these as high-risk changes

Require explicit review when any of these happen:

- changing the `AS OF` capture column of a `VERSIONED FOREIGN KEY`,
- changing the referenced table/domain/column of a `VERSIONED FOREIGN KEY`,
- adding a new entity around previously row-based tables,
- changing `ROOT`,
- changing `INCLUDES`,
- deleting an entity,
- renaming temporal token columns without preserving meaning,
- splitting one aggregate into several without a migration narrative for historical reads.

## 5. Run preflight warnings before rollout

If Studio schema diff is being used, treat warnings about entities and versioned foreign keys as mandatory review points, not informational noise.

Those warnings are there because the migration may be SQL-valid while still changing replay-visible semantics.

For CLI-driven rollouts, run `asqlctl -command migration-preflight` before approval. It now emits generated rollback SQL for reversible schema operations and flags statements that still require an explicit rollback narrative.

For online-safe additive columns, current support includes plain `ADD COLUMN` and `ADD COLUMN ... DEFAULT <literal> NOT NULL`. Treat the latter as a data backfill operation for review purposes, because replay and restart must preserve the same filled values for pre-existing rows.

## 6. Rehearse the explanation path

For affected tables, rehearse this before production rollout:

1. current view,
2. `resolve_reference(...)`,
3. `row_lsn(...)` or `entity_version(...)`,
4. `FOR HISTORY`,
5. `AS OF LSN` reconstruction.

If the team cannot explain the before/after story clearly, the migration is not ready.

## 7. Keep the migration package together

A reviewable migration package should include:

- forward SQL,
- rollback SQL (generated or explicit, but always reviewed),
- temporal-semantics review notes,
- verification queries,
- replay/history parity checks.

When using `ADD COLUMN ... DEFAULT <literal> NOT NULL`, include at least one replay/restart parity assertion that validates both pre-existing rows and newly inserted rows observe the same column value semantics after recovery.

## 8. Approval rule

Approve quickly only when all of these are true:

- SQL change is valid,
- rollback path is explicit,
- temporal meaning is unchanged or intentionally documented,
- replay/history-visible impact is understood,
- affected teams know whether the model remains row-based or entity-based.
