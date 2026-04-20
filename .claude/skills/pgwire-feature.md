---
name: pgwire-feature
description: Checklist for adding or modifying a pgwire feature (new SQL statement, new column type, binary encoding) without breaking extended-protocol clients like pgx.
---

# pgwire-feature

Use this when you add or modify anything that changes the wire-visible
behavior of `internal/server/pgwire`. The PostgreSQL extended query
protocol has sharp edges that will bite silently if you don't respect
them.

## The extended protocol flow (what the server must honor)

```
Client                  Server
  Parse    ────→
                 ←────  ParseComplete
  Describe S ──→
                 ←────  ParameterDescription
                 ←────  RowDescription  (or NoData for non-SELECT)
  Sync    ────→
                 ←────  ReadyForQuery
  Bind    ────→
                 ←────  BindComplete
  [Describe P] →       (pgx v5.9+ SKIPS this for cached statements)
                 ←────  RowDescription  (only if Describe P was sent)
  Execute  ────→
                 ←────  DataRow* CommandComplete  (or PortalSuspended)
  Sync    ────→
                 ←────  ReadyForQuery
```

## The pgx v5.9 trap

**pgx v5.9's `ExecStatement` does not send `Describe Portal` for cached
prepared statements.** It then calls `readUntilRowDescription`, which
blocks until the server sends `RowDescription`, `DataRow`, or
`CommandComplete`.

If your Execute handler blocks before sending *anything* (waiting for a
WAL event, a lock, a remote RPC, a timer), **the client deadlocks**.

**Rule**: if a streaming Execute path can block before producing a row,
send `RowDescription` eagerly at the start of the handler, guarded by a
flag so you don't double-send when `Describe Portal` *was* sent.

Reference implementation: `streamExtendedTailEntityChanges` in
`internal/server/pgwire/extended_query.go`, plus the `rowDescriptionSent`
field on `tailEntityChangesPortalState`.

## Result format codes

The `Bind` message carries `ResultFormatCodes`. Protocol rules:

| len(codes) | Meaning |
|---|---|
| 0 | all columns text |
| 1 | that code applies to all columns |
| N>1 | indexed per column |

Helper: `resultFormatCodeFor(codes, i)` in `extended_query.go`.

When the code for a column is `1` (binary), you must serialize that
column's literal via `literalToBinary(lit, oid)`, not `literalToText`.

## OID cheat sheet

| OID | Type | Binary encoding |
|-----|------|-----------------|
| 16 | bool | 1 byte (0 or 1) |
| 20 | int8 (int64) | 8 bytes big-endian |
| 21 | int2 (int16) | 2 bytes big-endian |
| 23 | int4 (int32) | 4 bytes big-endian |
| 25 | text | UTF-8 bytes |
| 114 | json | UTF-8 bytes |
| 1114 | timestamp | 8 bytes BE, **μs since 2000-01-01 UTC** |
| 1184 | timestamptz | same as 1114 |

Unix µs → PG µs: subtract `946_684_800_000_000`.

## Adding a new SQL statement

Minimum checklist:

1. **Parser**: add the grammar in `internal/engine/parser`. Write
   round-trip tests (parse → format → parse).
2. **AST**: add the node type. Keep it immutable.
3. **Executor**: decide whether it's read-only, WAL-writing, or DDL.
   Writing paths go through the standard commit path — don't invent a
   side channel.
4. **pgwire routing**: if the client will see results, add routing in
   `handleBind` (to mark the portal as SELECT-shaped) and in
   `handleExtendedExecute` (to dispatch). If it has its own column
   shape, add a case to `describeFields()` so `handleDescribe` can emit
   a real `RowDescription`.
5. **Simple query protocol**: if you only support extended, document it.
   Otherwise wire it up in `handleSimpleQuery` too.
6. **Tests**: pgwire conformance tests go in
   `internal/server/pgwire/extended_query_conformance_test.go` and
   adjacent test files. Cover both "has results" and "empty results".

## Adding a new data type

1. Pick the OID (reuse a PostgreSQL one if semantically compatible).
2. Extend `describeFields()` to emit it.
3. Extend `literalToText` and `literalToBinary` to serialize it.
4. Update the OID cheat sheet in this skill file.
5. Add a round-trip test: write via SQL → read via extended protocol
   with `ResultFormatCodes=[1]` → assert binary bytes → with `[0]` →
   assert text.

## Cancel / timeout

When a query must become cancelable, it must run under the context
returned by `state.beginQuery()`. Test it with pgx's `context.WithTimeout`
— pgx opens a separate TCP connection to send `CancelRequest` with
`(processID, secretKey)`.

**`SecretKey` is `[]byte` in pgx v5.9+, not `uint32`.** Always use the
`uint32ToSecretKey` / `secretKeyToUint32` helpers at the pgproto3
boundary.

## Don't do these things

- Don't bypass the portal struct for per-Bind state — the Execute path
  must find everything there.
- Don't send `BindComplete` before validating the bind params.
- Don't emit a `RowDescription` that disagrees with the `DataRow` values
  shape. pgx will decode positionally and produce garbage.
- Don't assume pgx sent `Describe Portal`. It usually didn't.
- Don't flush after every byte. Flush at message boundaries and when
  the client needs to unblock.
