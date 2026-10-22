# ADR 0008: Inclusion and consistency proofs

- Status: Accepted. Phase 3 of the [verifiable queries roadmap](../architecture/verifiable-queries-roadmap-v1.md).
- Date: 2026-10-21
- Decision drivers:
  - a third party should be able to check that **a set of rows, cited by primary key,
    had given values at a published state** without seeing any other row, and that **today's published log
    extends yesterday's** without seeing the records in between
  - proofs must tie to the signed digests of ADR 0005, so they inherit their anchor
  - no measurable cost on the commit path (ADR 0005 § Performance), and nothing the
    engine's queries can observe (AGENTS.md rules 1-6)
  - prefer an existing, audited format and implementation over a new one

## Context

1. **The chain proves order, not membership.** ADR 0005's hash chain lets a verifier
   with the WAL check that it was not altered, and a digest pins it. But showing that
   a record is in the log, or that one digest's history extends another's, means
   handing over every record in between.
2. **The ledger leaves are already RFC 6962 leaves.** A leaf is
   `SHA-256(0x00 || fields)`, independent of the LSN. That is the RFC 6962 (and
   RFC 9162) record hash, so a Merkle tree over the leaves in LSN order is an RFC 6962
   log with no change to the WAL. `golang.org/x/mod/sumdb/tlog`, which `sum.golang.org`
   runs on, implements that tree with tiled storage, inclusion proofs and consistency
   proofs. Its `RecordHash` and `NodeHash` match RFC 6962, which was checked when this
   ADR was written.
3. **WAL records do not carry rows.** An UPDATE or DELETE is stored as a predicate. A
   proof that "row 42 had result WITHIN_SPEC" cannot come from the WAL tree alone: it
   needs the row images, which the audit store holds (`OldRow`, `NewRow`,
   `CommitLSN`) and replay reproduces deterministically.
4. **"Had a value at state H" is a statement about the state, not a history entry.**
   Showing that a version of row 42 was written at LSN 100 does not show that it was
   still current at H = 500. That needs a commitment to the whole state at H, which
   can also prove that a row did *not* exist.
5. **What is published is a set of rows.** In the pharma integration, a finding is
   supported by its lineage: tens of artifacts across six levels (sources,
   extractions, discrepancies, resolutions, runs) and the edges between them, in two
   tables. Reports cite rows by primary key (UUID v7), not by WAL position. One proof
   per row means N proofs and N verifications per citation; one proof for the whole
   set against one digest is what can go into an exported report. The same store
   holds other questions and unpublished work, so the proof must reveal nothing
   about rows outside the set. Later, a report must also be able to show that it
   lists *all* the parents of an artifact (completeness upwards), never all its
   children: hiding a disagreeing input is what provenance exists to prevent, while
   other uses of a source are none of the report's business.
6. **Row values are often low-entropy.** `result IN ('WITHIN_SPEC', 'OUT_OF_SPEC')`. A
   proof that reveals the hash of a neighbouring row lets anyone who can guess its key
   test both values offline. Hiding the other rows needs salted leaves.

## Decision

Two trees, both built **off the commit path** by a background builder that follows
the WAL and the audit store, as the digest checkpointer does. Their roots are signed in
the digests, which gain a version 2.

### 1. Log tree: inclusion of a record, consistency between digests

- An RFC 6962 tree over the ledger leaves in LSN order. Tree size `n` equals the LSN
  of the last record covered.
- Built with `golang.org/x/mod/sumdb/tlog`, taken as a module dependency rather than
  vendored, so its fixes arrive with a version bump. Tiles of height 8, stored under the ledger
  directory, about 32 bytes per record. Tiles hold hashes only, so they outlive WAL GC.
- **The tree starts at LSN 1.** WAL GC (`WithWALGC`) never truncates past the last
  record the builder has added, and backups carry the tiles. A node whose WAL does not
  start at LSN 1 and has no tiles for the missing part (a data directory truncated
  before this ADR, or restored without tiles) does not build the tree: it logs why,
  keeps signing v1 digests, and proof requests fail with a clear error. Starting the
  tree at the first retained LSN instead would give two nodes different roots for the
  same LSN.
- **Digest v2** adds `tree_root` (at `tree_size = lsn`) next to `chain`.
- Proofs:
  - **consistency**: from digest A (size m) to digest B (size n), O(log n) hashes. It
    shows B's log contains A's unchanged, without any record.
  - **inclusion**: record at LSN i in the tree of a digest, O(log n) hashes.

### 2. State tree: a row's value at a checkpoint

- Only for tables marked for it: `CREATE TABLE … VERIFIABLE` or
  `ALTER TABLE … SET VERIFIABLE`, as `APPEND ONLY` is declared. Marking an existing
  table enters its live rows at that LSN; its proofs start at the first checkpoint
  after it. Other tables pay nothing.
- Only tables with a primary key can be verifiable: rows are found and proven by it,
  as reports cite them.
- A compact sparse Merkle tree over 256-bit keys. Keys and leaves carry a type byte,
  so other kinds of leaf can join the tree later without changing how rows are cited
  or hashed:
  - row key `SHA-256(0x01 || domain || table || canonical primary key)`, row leaf
    `SHA-256(0x00 || 0x01 || salt || canonical row)`, for every live row;
  - the canonical primary key is ADR 0006's typed value encoding of the primary key
    columns in key order (composite keys included). The canonical row is every column
    as (name, typed value) pairs sorted by name, so a leaf describes itself: verifying
    it needs no schema, and columns added or renamed later do not change how it is
    read. A value outside the key, such as a parent's `ordinal`, is proven with the
    row.
  - Hashing: an empty subtree is 32 zero bytes; a subtree holding one key is
    `SHA-256(0x02 || key || leaf)`, at the highest level where it is alone; any other
    subtree is `SHA-256(0x01 || left || right)`. Hashing one key per lonely subtree,
    instead of every one of its 256 levels, makes building the tree `O(n log n)`.
- **The root is a function of the engine's state at the checkpoint**, not of a log
  of row changes. `DROP TABLE`, `DROP COLUMN`, `RENAME COLUMN` and `ADD COLUMN …
  DEFAULT` change rows without writing row changes to the audit store, so a tree fed
  only by the audit store would disagree with the state. The node keeps the tree of
  the latest checkpoint in memory, with immutable nodes so the trees of recent
  checkpoints stay available. At each checkpoint it scans the verifiable tables' live
  rows, hashes again only those whose version changed (each row carries the LSN of the
  record that wrote it), and rebuilds a table whose columns or primary key changed.
  The scan is `O(rows)` per checkpoint and the hashing `O(changed rows)`; driving it
  from the audit store's row changes instead is a later optimisation.
  Proofs at an older checkpoint rebuild its tree from the historical state, which
  costs `O(rows of verifiable tables)`; nothing is persisted.
- **Digest v3** adds `state_root` at the digest's LSN; v1 and v2 digests keep
  verifying. Roots are kept, and proofs served, at **checkpoint LSNs only**: the LSNs
  digests are signed at.
- Proofs are **batch proofs**: one proof for a set of keys, in any verifiable tables,
  under one `state_root`, showing each key's row (inclusion of `(key, row, salt)`).
  The paths share their upper nodes, so each sibling hash appears once, and empty
  siblings are marked instead of sent. A single row is a batch of one.
- **No proofs of absence.** In a compact tree, proving that a key is absent can mean
  showing the key of another row whose path it shares, and keys are unsalted hashes
  of primary keys, so that would let a recipient test guessed keys. The integration
  does not need them; asking for a key with no row fails.
- What a batch proof reveals: the proven keys, their rows and salts, and hashes of
  other subtrees, which are salted and say nothing about the rows below them. Which
  siblings are empty does show roughly how many rows the tree holds: about log2 of
  it, an order of magnitude. That is accepted: the digest it is checked against
  carries its LSN, which already shows how many WAL records the node had written.
  Hiding it would mean making empty subtrees indistinguishable, with proofs several
  times larger.
- **Completeness upwards, later: group leaves.** A verifiable table may later declare
  a group (for `artifact_parents`, `child_id`). Each group gets a leaf of type `0x02`
  under `SHA-256(0x02 || domain || table || group column || canonical value)`, whose
  value hashes the sorted list of its rows' keys and leaves. Adding that leaf to a
  batch proves that the rows shown are all the group's rows. It is not built now; the
  type bytes keep it possible without changing rows or citations. A range proof over
  leaves ordered by primary key would do the same, but it must reveal the two keys
  bounding the range, such as the next artifact's UUID v7 and the time it carries;
  group leaves reveal nothing beyond the group.
- **Salt**: `HMAC-SHA-256(state key, key || LSN of the WAL record that wrote the row
  version)`, which the engine keeps with every row. The salt
  depends only on the key, the WAL and the secret, so replicas sharing the secret build
  the same tree. Leaves are salted from the first release of the state tree: adding
  salt later would change every leaf and root.
- **State key**: a 32-byte secret, separate from the signing key so that rotating the
  signing key does not change the tree. `asqlctl ledger-keygen` writes it next to the
  private key (`<key>.state`, mode 0600), and `asqld` loads it from there unless
  `-ledger-state-key` names another file. Replicas need the same file, as they need the
  same signing key.

### 3. Surfaces

Following ADR 0006:
- SQL: `asql_admin.prove_rows WHERE keys = '<JSON list of {domain, table, pk}>'
  [AND lsn = N]` returns the rows, their salts, one batch proof and the signed digest
  at that checkpoint; `asql_admin.prove_consistency WHERE from_lsn = … AND to_lsn = …`.
- CLI: `asqlctl prove-rows`, `verify-rows` and `verify-consistency`, which check
  proofs against digests and trusted keys only, with no data directory.
- It needs `ledger=signed`, like ADR 0006.

## What this proves, and what it does not

- **Proves**: node N signed that its state at LSN H had these rows with these values,
  and that the log of a later digest extends
  the log of an earlier one. Nothing else about the state or the log is revealed beyond
  the proven keys and the log's size.
- **Does not prove** that the values are true, nor that the state root was computed
  correctly from the log. A verifier with the data directory can rebuild the trees and
  compare roots, as `verify-result` re-executes a query.
- **Does not prove completeness, yet.** A batch proof shows that the rows it names
  exist with those values; it does not show that they are all the rows of some kind,
  for example that an artifact has no parents besides the edges shown. Group leaves
  (above) will add that for declared groups.
- **Does not hide** the keys that are proven, or the number of WAL records.

## Performance

- Log tree: about one node hash per record, amortized (~60 ns), plus tile writes, in
  the background builder.
- State tree: one path update per row change, about `log2(live rows)` hashes (20 for a
  million rows, ~1.3 µs), also in the builder.
- Commit path: unchanged. The builder lags the head; proofs exist only at signed
  checkpoints.
- Gates: as ADR 0005 — no commit-path regression, measured builder throughput and
  storage per million records.

## Alternatives considered

- **Only the log tree.** Cheap and standard, but cannot prove a row's value: WAL
  records are predicates (Context 3).
- **A tree over audit entries (a row-version log)** instead of a state tree. Proves
  that a version was written, not that it was still current at H, and cannot prove
  absence (Context 4).
- **Compute the trees on the commit path** so every LSN has a root. Adds hashing to
  every commit and stores a root per commit, for proofs at LSNs nobody signs.
- **Unsalted state leaves.** Simpler and shared without a secret, but leaks
  low-entropy neighbouring rows (Context 5).
- **A Merkle Patricia trie (Ethereum-style).** More complex and not standard outside
  Ethereum, for the same guarantees as a sparse Merkle tree here.

## Resolved questions

1. **Order.** The log tree ships first: it is smaller and standard, gives consistency
   proofs on its own, and settles digest v2 before the state tree builds on it.
2. **Checkpoints only.** Proofs are tied to signed digests, which exist only at
   checkpoints; a root per commit would cost on every commit for LSNs nobody signs. If
   a proof is needed right after a given commit, the answer is a checkpoint signed on
   demand, not a root per commit; that is left for later.
3. **Salted leaves.** Required: in the pharma use case, values like `WITHIN_SPEC` /
   `OUT_OF_SPEC` are guessable, and an unsalted proof would let its recipient test
   neighbouring rows. Retrofitting salt would need a new tree and digest version.
4. **Only `VERIFIABLE` tables.** Most tables (queues, configuration, scratch) need no
   proofs, and the persistent nodes' storage grows with every checkpoint.
5. **`golang.org/x/mod` as a dependency.** It is maintained by the Go team and runs
   `sum.golang.org`; vendoring would mean tracking its fixes by hand.

## Implementation plan

1. Log tree builder with `tlog` tiles, and its consistency and inclusion proofs, with
   tests against `tlog`'s own verifiers.
2. Digest v2 with `tree_root`; `verify-consistency`; v1 digests still verify.
3. `VERIFIABLE` tables, and the state key in `ledger-keygen` and `asqld`.
4. The compact state tree with immutable nodes, its key, leaf and salt encodings, and
   batch proofs, tested against the hashing rules written out independently; then the
   node's builder, which follows the engine's state at each checkpoint.
5. `state_root` in digest v3; batch proofs, `asql_admin.prove_rows`, `prove-rows` and
   `verify-rows`.
6. Benchmarks for the builder and storage, and confirmation that the commit-path
   benchmarks are unchanged.
