# Changelog

## v0.6.0 — 2026-10-22

### Upgrade notes

- **No downgrade to v0.5.0.** Snapshots move to format v18 (verifiable groups) and the
  WAL gains a DDL record v0.5.0 cannot replay (`ADD VERIFIABLE GROUP`). v0.6.0 reads
  everything v0.5.0 wrote.
- **`asql_admin.prove_rows` has a fourth column, `groups`,** after `digest`, `rows` and
  `proof` (`[]` when no group was asked for). A client reading the row positionally
  into three destinations must add one; `asqlctl prove-rows` handles it (#100).
- **`schema_snapshot` reports `verifiable_groups`** for tables that declare them (#99).

### Verifiable rows (ADR 0008): complete groups

- **Verifiable groups**: `ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id)
  REFERENCES artifacts (id)`. Both tables must be `VERIFIABLE`, and the referenced
  columns must be the referenced table's primary key, with the same types. It needs
  `ADMIN` and cannot be undone. `DROP COLUMN` of a column a group uses, and `DROP TABLE`
  of a referenced table, are refused, `CASCADE` included; `RENAME COLUMN` follows on
  both sides (#99).
- **Proofs that a group is complete**: every row of the referenced table has a group
  leaf in the state tree, an empty one when it has no rows. A `prove_rows` key entry
  `{"domain", "table", "group": [values]}` (with `"group_columns"` when the table has
  several groups) adds every row of that group and the group itself to the proof, so it
  shows all the parents of an artifact, and none for a source, without proofs of
  absence. `verify-rows` rebuilds each group from the proof's rows and lists it, for
  example `lab.artifact_parents(child_id=…): all 2 rows`; a row left out, or a source
  presented without its parents, makes it fail (#100).
- The node checks every row proof against the digest's state root before handing it
  out (#100).

## v0.5.0 — 2026-10-22

### Upgrade notes

- **No downgrade to v0.4.0.** Snapshots move to format v17 (verifiable tables and
  composite primary keys), and the WAL gains DDL records v0.4.0 cannot replay
  (`SET VERIFIABLE`, `ADD PRIMARY KEY`, and the matching `CREATE TABLE` flags).
  v0.5.0 reads everything v0.4.0 wrote.
- **`asql_admin.ledger_digest` has an eleventh column, `state_root`,** last, NULL
  unless the digest is version 3. A client reading the row with `SELECT *` into ten
  destinations now fails; name the columns (#96).
- **A node with a state key signs version 3 digests,** which also sign the state root
  of the `VERIFIABLE` tables (`state_root`, also in `GET /api/v1/ledger/digest`).
  Verifiers from v0.4.0 and earlier reject them: verify with this release's `asqlctl`.
  A node without a state key keeps signing version 2 (#96).
- **The state key.** `asqlctl -command ledger-keygen -output node.key` now also writes
  `node.key.state` (32 secret bytes, mode 0600) and reports it as `state_key` and
  `created` in its JSON. Run on an existing private key without a `.state` file, it
  writes only that file and leaves the key pair as it is; before, it failed with
  "file exists". `asqld` loads `<ledger-key>.state` unless `-ledger-state-key` names
  another file (`LedgerStateKeyFile` in `pkg/embed`): a file named with the flag, or a
  default file that exists, must load, while a missing default file only logs a
  warning. Every node of a cluster needs the same state key (#90).
- **`schema_snapshot` reports `primary_key`** as the list of key columns in key order,
  also for single-column keys (#93).

### Verifiable rows (ADR 0008)

- **Composite primary keys**: `PRIMARY KEY (child_id, parent_id)` as a table constraint,
  with `NOT NULL` key columns (`23502`), tuple uniqueness (`23505`) and `ON CONFLICT` on
  the key columns. `ALTER TABLE … ADD PRIMARY KEY (…)` adds one to an existing table,
  `APPEND ONLY` included, after checking the existing rows for NULLs and repeated
  tuples; it fails without changing anything when they break it (#93).
- **`VERIFIABLE` tables**: `CREATE TABLE … VERIFIABLE` (with `APPEND ONLY` in either
  order) or `ALTER TABLE … SET VERIFIABLE`. It needs a primary key (`42P16` without one)
  and `ADMIN`, restricts nothing, and cannot be undone (#91, #94).
- **State tree**: with `ledger=signed` and a state key, the node keeps a salted Merkle
  tree of the verifiable tables' live rows, off the commit path, and each digest signs
  its root (#95, #96).
- **Row proofs**: `SELECT * FROM asql_admin.prove_rows WHERE keys = '[{"domain", "table",
  "pk": [...]}, …]' [AND lsn = N]`, `asqlctl -command prove-rows -keys rows.json` and
  `verify-rows -proof proof.json -pubkey node.pub` prove a set of rows, cited by primary
  key, in one batch against a signed digest, revealing nothing about other rows beyond
  the digest's LSN and the rough size of the tree. They need the privileges of reading
  those tables, plus `SELECT_HISTORY` away from the head. A proof cannot show that a row
  is absent, nor yet that the rows shown are all of some kind (#97).
- `verify-ledger -digest` reports a version 3 digest's `state_root` as `not-checked`:
  rebuilding it needs the secret state key.

### Docs

- ADR 0008 amended: batch proofs by primary key, typed and group leaves, digest v3, the
  state tree as a function of the engine's state (#92, #95).

## v0.4.0 — 2026-10-21

### Upgrade notes

- **`asql_admin.ledger_digest` has a tenth column, `tree_root`,** last, NULL for
  version 1 digests. A client that reads the row with `SELECT *` into nine
  destinations now fails (pgx: "number of field descriptions must equal number of
  destinations"); name the columns or add one destination (#87).
- **Digests are version 2 with `ledger=signed`.** Each digest also signs the root of
  the node's log tree (`tree_root`, also in `GET /api/v1/ledger/digest`). Verifiers
  from v0.3.0 and earlier reject them (`unsupported version 2`): verify with this
  release's `asqlctl`, which accepts both versions. Digests signed before keep
  verifying (#87).
- **A JSON path compared with a column works.** `payload->>'k' = other_col` and
  `other_col = payload->>'k'` (and `!=`, `<`, `>`, …) failed with `42601`
  "unsupported literal"; they now return their rows, in `SELECT`, `UPDATE` and
  `DELETE`. A row without that path matches nothing, not even with `!=`. Comparing
  two JSON paths with each other still fails with `42601` (#85).
- **A new file in the ledger directory**, `log-tree.hashes`, with `ledger=signed`:
  about 64 bytes per WAL record. The node builds it from the WAL on first start, off
  the commit path. A node whose WAL does not start at LSN 1 keeps signing version 1
  digests and logs why.
- No WAL or snapshot format change. v0.3.0 can still open a v0.4.0 data directory; it
  ignores the tree file and signs version 1 digests again.

### Verifiable history

- **Log tree** (ADR 0008): an RFC 6962 Merkle tree over every WAL record, built off
  the commit path; WAL GC never removes records it has not added (#86, #87).
- **`verify-ledger -digest`** rebuilds the tree from the WAL and checks a version 2
  digest's `tree_root`: `match`, `mismatch` (failure `digest-tree-mismatch`) or
  `not-checked` (#87).
- **Consistency proofs between two signed digests**, which show that the later log
  extends the earlier one without revealing any record:
  `SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = A [AND to_lsn = B]`
  (`ADMIN`), `asqlctl -command prove-consistency` and `verify-consistency`, which
  needs only the trusted keys (#88).

### Docs

- ADR 0008 accepted, with its decisions: log tree first, proofs at signed digests,
  salted state tree for `VERIFIABLE` tables later (#84).

## v0.3.0 — 2026-10-21

### Upgrade notes

- **One process per data directory.** A WAL store now takes an exclusive lock on
  `.<wal base>.lock` next to the segments and holds it until `Close`. A second
  `asqld`, or a second `embed.Start` in the same process, on a directory in use
  fails with `wal.ErrDataDirInUse` (`embed.ErrDataDirInUse`), naming the PID that
  holds it. Before, both started and wrote the same WAL. The OS releases the lock
  when the process exits, so a crash leaves nothing to clean up. `verify-ledger` and
  the audit readers do not take the lock and still work on a running node's
  directory (#80).
- **`AS OF` applies to the whole query, and only once.** Every table a query reads,
  in joins, CTEs, subqueries and both sides of `UNION`, `INTERSECT` and `EXCEPT`, is
  read at its `AS OF`. An `AS OF` written inside a CTE, a subquery or one side of a
  set operation used to be ignored and answer from the present; it now fails with
  `0A000`. Write it once, in the outermost `FROM`; it may now come before a `JOIN`
  (#82).
- **`TX_TIMESTAMP` is the commit time.** It used to be read from the clock
  separately; now it is the time the COMMIT record carries, the same for every row
  of a transaction, and it matches `asql_admin.timeline_commits.commit_time`, so
  `AS OF TIMESTAMP` with a row's value sees that row. In `TIMESTAMP` columns it is
  stored as a timestamp; earlier versions stored it as text, and rows written then
  keep their text values. Text columns keep RFC 3339 text. `INSERT … RETURNING`
  still returns the statement's time (#78).
- **Comparisons of an indexed column with a subquery, another column or a JSON path
  returned no rows**, and `UPDATE`/`DELETE` with them changed nothing, without an
  error. They now match the rows they should; a scalar subquery with more than one
  row fails with `21000` (#81).
- **Set operations outside a transaction work.** `UNION`, `INTERSECT` and `EXCEPT`
  failed with `25P01` outside a transaction; they now run as reads. Their sides
  combine by column position, as in PostgreSQL, and sides with a different number
  of columns fail with `42601`. An unparseable `SELECT` reports its syntax error
  (`42601`) instead of `25P01` (#82).
- No format change: v0.3.0 reads and writes the same data as v0.2.0, and v0.2.0 can
  read what v0.3.0 writes.

### Performance

- An uncorrelated scalar subquery compared with an indexed column runs once and
  uses the index (#81).

### Docs

- ADR 0008 proposes inclusion and consistency proofs for the ledger (#79).

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
