# Temporal introspection surface v1

This note defines the minimal supported temporal helper surface exposed through
pgwire.

## Supported scalar helpers

- `current_lsn()` → current committed head `LSN`.
- `row_lsn('domain.table', 'pk')` → current visible row-head `LSN` for a
  primary-key lookup.
- `entity_version('domain', 'entity', 'root_pk')` → latest visible entity
  version number.
- `entity_head_lsn('domain', 'entity', 'root_pk')` → commit `LSN` of the latest
  visible entity version.
- `entity_version_lsn('domain', 'entity', 'root_pk', version)` → commit `LSN`
  for a specific visible entity version.
- `resolve_reference('domain.table', 'pk')` → the token a versioned foreign key
  would auto-capture against the current committed snapshot.
- `TAIL ENTITY CHANGES domain.entity [FOR '<root_pk>'] [FROM LSN <n>] [TO LSN <n>] [LIMIT <n>] [FOLLOW]`
  → ASQL-native pgwire/query surface for replay-safe entity backlog inspection.

## Return semantics

- Helpers are deterministic scalar lookups.
- Missing rows or entities return SQL `NULL`.
- Missing entity versions return SQL `NULL`.
- Invalid argument shapes or semantic mismatches return explicit errors.
- `resolve_reference(...)` returns:
  - the latest entity version for entity root tables,
  - the current row-head `LSN` for non-entity tables.

## Scope

This surface is intentionally small. It supports debugging, temporal
introspection, and versioned-reference diagnostics without widening the engine's
public API more than necessary.

`TAIL ENTITY CHANGES` is not PostgreSQL `LISTEN`/`NOTIFY` compatibility.
It is an ASQL-native temporal surface over committed entity versions. In this
first slice it returns a finite backlog ordered by commit `LSN`, with one row
per committed entity-version transition:

- `commit_lsn`
- `commit_timestamp`
- `domain`
- `entity`
- `root_pk`
- `entity_version`
- `tables` as a JSON array of aggregate tables touched by that committed version

When you add `FOLLOW`, the command keeps the pgwire query open and emits
additional rows as new matching commits arrive.

It works on both simple query and extended query protocol. In extended query
mode, a portal can suspend and later resume across repeated `Execute` calls
without losing the `LSN` cursor.

Use it when you want to answer questions like:

- which aggregate revisions happened after a known `LSN`,
- which entity versions affected one root instance,
- which tables participated in a committed aggregate transition.
