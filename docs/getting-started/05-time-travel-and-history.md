# 05. Time Travel, History, and Debugging

ASQL treats historical inspection as a normal developer workflow.

## Read a past snapshot

You can query past state by `LSN`.

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command time-travel -domains app -lsn 4 \
  -sql "SELECT id, email FROM users"
```

## Supported temporal helper surface

These helpers are part of the supported product surface:

- `current_lsn()`
- `row_lsn('domain.table', 'pk')`
- `entity_version('domain', 'entity', 'root_pk')`
- `entity_head_lsn('domain', 'entity', 'root_pk')`
- `entity_version_lsn('domain', 'entity', 'root_pk', version)`
- `resolve_reference('domain.table', 'pk')`

## What each helper is for

### `current_lsn()`

Use it when you need the current committed head.

### `row_lsn(...)`

Use it when you need the current visible head `LSN` of one row.

### `entity_version(...)`

Use it when the table belongs to an entity aggregate and you want the current business-facing version.

### `entity_head_lsn(...)`

Use it when you want the commit `LSN` of the latest visible aggregate version.

### `entity_version_lsn(...)`

Use it when you already know an entity version and want the replay-safe `LSN` that corresponds to it.
This is the bridge between aggregate semantics and `AS OF LSN` reads.

### `resolve_reference(...)`

Use it to inspect what a versioned foreign key would capture right now.

## `FOR HISTORY`

ASQL exposes a stable `FOR HISTORY` contract.
The canonical metadata columns are:

- `__operation`
- `__commit_lsn`

Example:

```sql
SELECT * FROM users FOR HISTORY WHERE id = 1;
```

Use this when you want the chronological mutation trail for one row or a filtered set of rows.

## Recommended workflow for debugging

1. find the affected row,
2. inspect `row_lsn(...)` or `entity_version(...)`,
3. inspect `FOR HISTORY`,
4. run a targeted `AS OF LSN` query,
5. compare historical state with current state in Studio.

## Studio support

ASQL Studio now exposes:

- row mutation history,
- entity history,
- temporal metadata on row detail,
- the Time Explorer for snapshot, diff, and history workflows.

## Next step

Continue with [06-entities-and-versioned-references.md](06-entities-and-versioned-references.md).
