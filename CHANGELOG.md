# Changelog

## v0.2.0 — 2026-10-21

### Upgrade notes

- **Joins that filtered on the joined table could return no rows.** When the base
  table had an index on the join column, a WHERE condition on the joined table other
  than `=`, `<`, `>`, `<=`, `>=`, `IS NULL` or `IN` (for example `!=`, `LIKE`,
  `BETWEEN`, `NOT`) dropped every row, without an error. Such queries now return their
  rows (#75).
- **Subqueries in WHERE report their errors.** These used to read as false; they now
  fail:
  - a scalar subquery that returns more than one row (`21000`);
  - a subquery with more than one column (`42601`);
  - a subquery that fails, for example on a missing table (`42P01`) (#74).
- No format change: v0.2.0 reads and writes the same data as v0.1.0, and v0.1.0 can
  read what v0.2.0 writes.

### Performance

- **Uncorrelated subqueries run once per query** instead of once per outer row, and an
  `IN (SELECT …)` uses the index of the outer column. Over 100,000 rows:
  `IN (SELECT …)` 3.1 s → 65 µs; `IN` over a recursive CTE 930 ms → 135 µs (#74).
- **A join with the big table first** reads only the rows of the big table that can
  match, through its index: 39 ms → 0.13 ms, as fast as with the small table first.
  The rows and their order are unchanged (#76).

### Fixes

- A subquery's value is its select list's first column; it used to come from whichever
  key a map iteration gave first. Unqualified tables in a subquery resolve to the same
  domain on every run (#74).

## v0.1.0 — 2026-10-21

The first tagged release. Earlier builds were taken from `main`; a binary built
from this tag reports `v0.1.0` as its engine version, which result attestations
sign.

### Upgrade notes

Read these before pointing v0.1.0 at a data directory written by an earlier build.

- **No downgrade.** Snapshots move to format v15, the WAL gains new literal tags,
  a DML op code (`0x06`) and a commit header in COMMIT records. Earlier builds
  cannot read what v0.1.0 writes. v0.1.0 reads everything earlier builds wrote.
- **Timestamps written by earlier builds were lost on restart** (they read back as
  1970-01-01 from the WAL, snapshots and history). Values already restarted past
  cannot be recovered; a node that still holds them in memory writes them correctly
  in its next snapshot.
- **pgx over `simple_protocol`** stored every argument as text, also in `INT`,
  `BOOL`, `FLOAT` and `TIMESTAMP` columns. Rows written that way keep their text
  values, which typed comparisons (`WHERE id = '2'`) no longer match. Start from a
  fresh data directory, or rewrite them.
- **`AS OF TIMESTAMP`** resolves by the wall-clock time commits record from this
  release on. Commits written before it have none: a date that depends on them fails
  instead of guessing; `AS OF LSN` still reads them.
- **`!=` / `<>`** used to match nothing. Statements the WAL keeps as SQL text (an
  UPDATE or DELETE whose predicate also has `LIKE`, `BETWEEN`, a subquery or JSON) are
  re-parsed on replay, so one that used `!=` now matches the rows it should have.
- **`CREATE TABLE` is stricter**: unknown text after the column list is an error
  instead of being ignored.
- **Queries that silently returned less now fail or answer in full**: a select-list
  column that does not exist (`42703`), an expression ASQL cannot compute (`0A000`),
  `FOR HISTORY` on a table that never existed (`42P01`), and failing `asql_admin`
  queries, which returned an empty result.

### Verifiable history

- **Tamper-evident WAL ledger** (ADR 0005): a hash chain over every record, anchors in
  segment headers and snapshots, `asqlctl -command verify-ledger` (#35).
- **Signed ledger digests** with `ledger=signed`: Ed25519, a digest sidecar log,
  `asql_admin.ledger_digest`, `GET /api/v1/ledger/digest`, `verify-ledger -digest` (#42).
- **Compliance profile**: `asqld -compliance` refuses to start unless the ledger vouches
  for the whole retained WAL (#49).
- **Signed query results** (ADR 0006): `asql_admin.attest_query` returns a SELECT's
  rows with a signed attestation bound to the chain at a committed LSN;
  `asqlctl attest-query` and `verify-result` check signature, rows, chain and
  re-execution apart (#60, #61, #67).
- **Commit wall-clock times** (ADR 0007): each commit records its time, covered by the
  ledger; `AS OF TIMESTAMP '<date>'` returns the state at that date, and
  `asql_admin.timeline_commits` shows `commit_time` (#66).

### SQL and PostgreSQL compatibility

- **Append-only tables**: `CREATE TABLE … APPEND ONLY`, `ALTER TABLE … SET APPEND ONLY`,
  irreversible, for every principal (#55).
- **Per-row write conflicts**: concurrent transactions on different rows of a table
  both commit (#56).
- **`WITH RECURSIVE`**, CTE column lists, and qualified columns in CTE bodies (#59).
- **PostgreSQL type names**: `INTEGER`, `BIGINT`, `BOOLEAN`, `REAL`,
  `DOUBLE PRECISION`, `FLOAT4`/`FLOAT8`, … (#52).
- **Quoted literals take the type of their column**, as in PostgreSQL; invalid input
  fails with `22P02` (#58).
- **`!=` and `<>`** in WHERE (#62).
- **Select-list expressions**: literals and arithmetic are computed, columns are named
  by their alias also over the simple protocol (where an alias returned NULL), and
  recursive CTEs can count depth (#70).
- **`asql_admin` tables** apply `ORDER BY`, `LIMIT` and `OFFSET`, and report errors
  instead of empty results (#71).
- **`FOR HISTORY` runs the whole SELECT**: aggregates, `GROUP BY`, `ORDER BY`, and
  `__operation` / `__commit_lsn` in WHERE (#54).
- **A failed COMMIT ends the transaction**, as in PostgreSQL (#51).

### Embedding and tools

- **`pkg/embed`** runs the server inside a Go process; `pkg/servertest` gives tests an
  empty store each (#53).

### Fixes

- pgx: `FLOAT` and aggregates scan into Go types; NULL parameters, parameter typing by
  column, `Ping`, timestamps with microseconds and as binary parameters, `$n` inside
  literals, and parameter values containing `$n` (#52, #54, #58, #61, #64).
- Engine: replay atomicity, reproducible `FOR HISTORY`, unique TxIDs across restarts
  and leaders, uncommitted `INSERT … RETURNING`, timestamp index and snapshots on
  followers (#40, #41, #43, #44, #47).
- Raft: conflicting log suffix rollback, commit index on restart, leader readiness and
  step-down on lost quorum (#37, #38, #39).
- Storage: timestamp values in the WAL, snapshots and row history (#57).

### Determinism

A determinism suite (`test/determinism`) compares everything query-visible across
every way of rebuilding state, commit times and date resolution included (#43, #66).
