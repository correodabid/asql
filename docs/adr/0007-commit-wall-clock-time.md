# ADR 0007: Record the wall-clock time of each commit

- Status: Accepted.
- Date: 2026-10-20
- Decision drivers:
  - `AS OF TIMESTAMP '<date>'` must answer with the state at that date, or fail; it
    must never answer with another state. A report "as of date X" that quotes numbers
    that did not exist then is worse than an error (ASQL-9).
  - The time must be part of the history, so that every replay, replica and snapshot
    resolves a date to the same commit (AGENTS.md rules 1 and 6).
  - It must be covered by the ledger (ADR 0005), so that signed digests also vouch
    for when commits happened, as far as the node's clock can tell.

## Context

1. **WAL timestamps are logical.** Each record's `Timestamp` is a counter that grows by
   one per record (`engine.logicalTS`). It orders records, and the gRPC and HTTP APIs
   and the `/* as-of-ts: N */` hint take it explicitly as a logical timestamp.
2. **`AS OF TIMESTAMP '<date>'` compared a date with that counter.** The parser turned
   the date into Unix microseconds, and the engine resolved them as a logical
   timestamp. Any real date lay past the end of the log, so the query returned the
   present. #63 makes it fail with `0A000` instead; this ADR makes it work.
3. **No wall-clock time of a commit is recorded anywhere.** The only wall-clock value
   in the WAL is the `TX_TIMESTAMP` default, read when a statement is queued, which is
   column data, not commit metadata.
4. **The COMMIT record has an empty payload.** Its leaf hash covers the payload
   (ADR 0005), and replay does not read it.

## Decision

### 1. The commit time travels in the COMMIT record

The leader, when it builds a transaction's WAL records in the commit queue, sets the
COMMIT payload to a versioned commit header:

```
[0x01 version] [i64 commit time, Unix microseconds, UTC]
```

The time comes from the engine's injected clock (`platform/clock`), never from
`time.Now()` in replay. It is read once, on the leader, and followers, replay and
snapshots read it back from the record, like the volatile defaults (`UUID_V7`) the WAL
already materializes. Because the leaf hash covers the payload, the chain and every
signed digest cover commit times.

### 2. Commit times never go backwards

`commit_time = max(clock.Now(), previous commit time)`, where the previous commit time
is the last one in the WAL, restored on replay. Commit times are therefore
non-decreasing in LSN order, also across leader changes, because a new leader holds
the same log. A clock that jumps back makes commits share a time; it never reorders
them. This is what lets a date resolve to one commit by binary search.

### 3. Resolving `AS OF TIMESTAMP t`

- `t` is a timestamp literal. Without a zone it is UTC.
- The state at `t` is the state after the last commit whose time is `<= t`.
- If `t` is earlier than the first commit with a recorded time, and commits without
  one (written before this change) precede it, the query fails: the state at `t`
  cannot be told from the WAL. The error names the first LSN and time that can be
  resolved.
- If no commit is that old and none lacks a time, the state at `t` is the empty
  database, so the tables do not exist yet, as at LSN 0.
- A `t` after the last commit resolves to the head.

The logical timestamp APIs and the `/* as-of-ts: N */` hint are unchanged.

### 4. Index

Commit times are kept next to the existing timestamp index, which already maps WAL
timestamps to LSNs, is persisted, and is rebuilt from the WAL when it falls behind.
`asql_admin.timeline_commits` gains a `commit_time` column, so a reader can see which
commit a date resolves to.

## Consequences

- **Format.** COMMIT records written from now on carry 9 payload bytes. Older WAL
  records keep an empty payload and have no time. Versions that predate this change
  ignore the payload of a COMMIT record: checked by opening a data directory written
  with commit times, from its snapshots and from its WAL alone, with the version
  before this change, which read it and wrote to it.
- **Downgrade and upgrade again.** Commits an older version writes have no time,
  after commits that do. A date that falls among them fails like a date before the
  first recorded commit, instead of resolving to the timed commit before them.
- **Precision and trust.** The time is the leader's clock, asserted by the node. It
  is as good as that clock, and the clamp in § 2 means it can lag after a clock jumps
  back. Proving that a state existed by a date to a third party still needs a signed
  digest published or time-stamped elsewhere (ADR 0005); the commit time says when
  the node committed it, not that it did so before anyone else could know.
- **Determinism.** Replay never reads the clock: the time is WAL data. The
  determinism suite gains commit times in its fingerprint, and a test that runs the
  workload with a clock that goes backwards.

## Alternatives considered

- **Keep refusing `AS OF TIMESTAMP`** (#63). Safe, but leaves users to map dates to
  LSNs themselves, with nothing in the database to map them with.
- **A new field in every WAL record header.** Needs a record format version, a
  migration and more bytes per record, for a value only commits need.
- **Store the time only in a sidecar index, not in the WAL.** Replicas and replays
  would each assign their own times, and two nodes could resolve the same date to
  different commits. The ledger would not cover it either.
- **Reuse `TX_TIMESTAMP`.** It is column data that only some tables have, read when a
  statement is queued rather than when the transaction commits.
