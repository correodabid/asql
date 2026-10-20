# ADR 0006: Sign query results bound to a committed ledger state

- Status: Proposed. Phase 2 of the [verifiable queries roadmap](../architecture/verifiable-queries-roadmap-v1.md).
- Date: 2026-10-20
- Decision drivers:
  - an auditor or counterparty should be able to receive the answer to a query and later check which node gave it, against which published state, and whether the answer is right
  - the answer must be tied to a state that is already committed to elsewhere (a ledger digest, ADR 0005), so the node cannot later claim a different history
  - it must not change the determinism contract (AGENTS.md rules 1-6), and must not touch the commit path
  - it should reuse the ADR 0005 keys, encodings and verification tooling rather than add a second trust system

## Context

ADR 0005 gives every committed LSN `n` a chain value `chain_n` and lets a node sign
digests `{lsn, chain, ...}` of its committed head. A digest proves which history a node
had; it says nothing about what a query over that history returns.

Facts that constrain the design:

1. **Reads at a fixed state already exist.** `TimeTravelQueryAsOfLSN` replays to an LSN
   and runs a SELECT there. Replay is deterministic and so is the order of the rows it
   returns (`test/determinism`, `TestRepeatedReplaysAreIdentical`), so the same query at
   the same LSN gives the same rows on any replica.
2. **Chain values at arbitrary LSNs are not at hand.** A store keeps the last 4,096
   `(LSN, chain)` points in memory. Older values must be recomputed from the nearest
   anchor, a sealed segment header's `chainBeforeFirstLSN`, which means rehashing at most
   one segment.
3. **Query authorization lives in pgwire.** Principals and privileges (ADR 0004) are
   enforced on pgwire sessions. The admin HTTP API authenticates with bearer tokens,
   not principals, so it cannot apply a principal's read privileges.
4. **A signature alone proves little.** The VQP review (see the roadmap) found that a
   responder signing its own answer over its own data proves only that it said so. What
   gives the answer weight is that a third party can tie it to a state committed to
   elsewhere and, with access to the data, re-execute it.

## Decision

A node in `ledger=signed` mode can return, for a SELECT at a committed LSN, the result
together with a signed **result attestation**:

```
{ version: 1, lsn, chain, term, timestamp,
  domains, sql, result_hash, row_count, columns,
  engine_version, node_id, key_id, signed_at,
  sig: Ed25519(canonical bytes) }
```

### 1. What is bound

- **The state.** `lsn` is a committed LSN: the engine's applied head by default, or an
  `AS OF LSN` the caller names, up to the head. `chain` is `chain_lsn`; `term` and
  `timestamp` are those of the WAL record at `lsn`, as in a digest. A verifier holding
  any digest at or after `lsn`, or the WAL itself, can check that `chain` belongs to
  that history.
- **The question.** `sql` is the exact SELECT text the engine executed, and `domains`
  the sorted domains it ran in. Placeholders are not accepted in v1, so the text is
  the whole question. Writes, `FOR HISTORY` and the virtual `asql_admin` tables are
  rejected: they are not functions of the state at `lsn`.
- **The answer.** `result_hash` is SHA-256 over the canonical encoding of the result
  (below), and `row_count` and `columns` are included so a reader can check the shape
  without recomputing. The rows themselves travel next to the attestation, not inside
  the signed bytes.
- **The software.** `engine_version` is signed because re-executing with a different
  engine version is not guaranteed to give the same rows. Verifiers report a version
  mismatch instead of a failure.

### 2. Canonical encodings

Both are fixed big-endian byte strings, not JSON, for the same reason as digests.

- **Result.** `asql-result-v1`, a zero byte, `u32` column count, each column name
  length-prefixed, `u64` row count, then each row as its values in column order. A
  value is a 1-byte kind (null, int, float, text, bool, timestamp, json) followed by a
  fixed-width or length-prefixed payload: `i64` for int and timestamp (Unix µs),
  IEEE-754 bits for float, UTF-8 bytes for text, JSON text as stored. Row order is the
  engine's order, which is deterministic; a query that wants a particular order says
  `ORDER BY`.
- **Attestation.** `asql-result-attestation-v1`, a zero byte, then the fields in the
  order above, strings and lists length-prefixed, `signed_at` as Unix ns.

### 3. Where it is computed

Outside the engine, in the pgwire layer, like digests. The server runs the query with
the session's principal through the existing time-travel path, encodes and hashes the
result, reads `chain_lsn` from the WAL store and signs with the node's
`ports.LedgerSigner`. Nothing is written to the WAL or to engine state. Signing happens
per request, never on the commit path; at roughly 20 µs per Ed25519 signature the cost
is dominated by the query itself.

`SegmentedLogStore` gains `ChainAt(lsn)`. It answers from the in-memory ring when it
can, and otherwise rehashes from the nearest earlier segment-header anchor. It fails
for an LSN beyond the committed head or before the retained WAL.

### 4. Surfaces

- **SQL:** `SELECT * FROM asql_admin.attest_query WHERE sql = '...' [AND lsn = N]`,
  following `asql_admin.row_history`. It returns one row with the attestation fields
  and the result rows as a JSON array of arrays (`rows`), so pgwire-only clients get
  everything in one round trip. It requires the privileges of the inner query, plus
  `SELECT_HISTORY` when `lsn` is not the head, as other historical reads do.
- **CLI:** `asqlctl -command attest-query -sql '...' [-lsn N] -out result.json` runs
  the above and writes the attestation and rows as JSON.
  `asqlctl -command verify-result -result result.json -pubkey keys.pem [-digest d.json] [-data-dir D]`
  checks, in order:
  1. the signature, against the trusted keys only;
  2. `result_hash`, `row_count` and `columns` against the rows in the file;
  3. with `-digest` (verified as in ADR 0005): that the digest is at `lsn` with the
     same chain, or, with `-data-dir`, that the WAL's chain at `lsn` matches and
     extends to the digest;
  4. with `-data-dir`: re-executes the query at `lsn` on a scratch copy and compares
     the result hash. This is the step that shows the answer is *right*, not just
     signed.

  Each step's outcome is reported separately, so a verifier without the data can
  still check 1-3.
- **Admin HTTP:** not in v1, because it has no principal to authorize the inner query
  with (Context 3).

### 5. Configuration

Only available with `ledger=signed`; otherwise the table returns an error that says
so. No new flags.

## What this proves, and what it does not

- **Proves**, with steps 1-3: node `node_id` claimed that `sql` over the history ending
  at `chain` returned these rows, and that history is the one a digest committed to.
  The node cannot later deny the claim, and it cannot have answered from a different
  history than the one it published.
- **Proves**, with step 4: the answer is correct for that history.
- **Does not prove** that the rows are correct without step 4. Without access to the
  data, a verifier relies on the node's honesty for the answer itself. Removing that
  reliance without revealing the data is phase 3 (inclusion proofs) and phase 4
  (attested or zero-knowledge execution).
- **Does not hide data.** The rows are in the clear. Selective disclosure is phase 3.
- **Does not prove the data is true.** As the roadmap says, the integrity of history is
  not the truth of the data.

## Alternatives considered

- **Sign every result transparently.** Rejected. It would add a signature to every
  read, need a protocol extension pgwire clients do not understand, and sign results
  of queries that are not functions of a committed state.
- **Put the rows inside the signed bytes.** Rejected. Signing the hash gives the same
  binding, keeps the signature small and lets large results be streamed or stored
  apart from the attestation.
- **Hash the pgwire text encoding of the rows.** Rejected. It depends on client and
  server formatting choices (float text, timestamp precision), so the hash would not
  be reproducible across tools or engine versions.
- **Accept the current, unapplied WAL head.** Rejected for the same reason as for
  digests (ADR 0005 § 4): a new Raft leader can still replace it.

## Open questions

1. Should attestations also be appended to a sidecar log like digests, so the node
   keeps evidence of what it attested? The proposal is no: the requester keeps it.
2. Should `verify-result` step 4 run in-process on a copy of the data directory, as
   proposed, or against a running replica?
3. Is the admin HTTP surface needed in v1 for non-pgwire clients, which would require
   principal authentication there first?

## Implementation plan

1. `wal.ChainAt` with tests against `VerifyLedger`'s recomputed chain.
2. Result encoding and hashing, attestation encoding, signing and verification, with
   format vectors like ADR 0005's.
3. `asql_admin.attest_query` with authorization, over both pgwire protocols.
4. `asqlctl attest-query` and `verify-result`, including re-execution, and an
   end-to-end test that tampers with each of rows, hash, signature, LSN and chain.
5. Benchmarks: attestation latency against the plain AS OF query at the head and at
   an old LSN, and confirmation that the commit-path benchmarks are unchanged.
