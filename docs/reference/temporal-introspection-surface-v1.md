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
