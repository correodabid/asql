# ADR 0008: Inclusion and consistency proofs

- Status: Accepted. Phase 3 of the [verifiable queries roadmap](../architecture/verifiable-queries-roadmap-v1.md).
- Date: 2026-10-21
- Decision drivers:
  - a third party should be able to check that **one row had a given value at a
    published state** without seeing any other row, and that **today's published log
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
5. **Row values are often low-entropy.** `result IN ('WITHIN_SPEC', 'OUT_OF_SPEC')`. A
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
- A sparse Merkle tree keyed by `SHA-256(domain || table || primary key)`, holding
  `leaf = SHA-256(0x00 || salt || canonical row)` for every live row. The canonical row
  is ADR 0006's typed value encoding, in column order.
- Updated from the audit store's row images in commit order: INSERT and UPDATE set the
  key, DELETE clears it. The tree is persistent (copy-on-write nodes), so every
  checkpoint root stays provable.
- **Digest v2** adds `state_root` at the digest's LSN. Roots are kept, and proofs
  served, at **checkpoint LSNs only**: the LSNs digests are signed at. Old checkpoints
  can be pruned with their digests.
- Proofs: **inclusion** of `(key, row, salt)` under `state_root`, and **non-inclusion**
  of a key. Only the proven row's salt is revealed.
- **Salt**: `HMAC-SHA-256(state key, key || commit LSN of the row version)`. The salt
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
- SQL: `asql_admin.prove_row WHERE domain = … AND table = … AND pk = … [AND lsn = N]`
  returns the row, its salt, the proof and the signed digest at that checkpoint;
  `asql_admin.prove_consistency WHERE from_lsn = … AND to_lsn = …`.
- CLI: `asqlctl verify-row` and `verify-consistency`, which check proofs against
  digests and trusted keys only, with no data directory.
- It needs `ledger=signed`, like ADR 0006.

## What this proves, and what it does not

- **Proves**: node N signed that its state at LSN H had row R with these values (or
  no row with that key), and that the log of a later digest extends the log of an
  earlier one. Nothing else about the state or the log is revealed beyond the proven
  key's existence and the tree sizes.
- **Does not prove** that the values are true, nor that the state root was computed
  correctly from the log. A verifier with the data directory can rebuild the trees and
  compare roots, as `verify-result` re-executes a query.
- **Does not hide** the key that is proven, or the number of records.

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
4. State tree with persistent nodes, built from the audit store, rebuilt from the WAL
   when the audit store is.
5. `state_root` in digest v2; `asql_admin.prove_row` and `verify-row`.
6. Benchmarks for the builder and storage, and confirmation that the commit-path
   benchmarks are unchanged.
