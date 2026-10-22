# Verifiable queries roadmap (v1)

Status: working direction, not a commitment. The durable decision for phase 1 lives in
[ADR 0005](../adr/0005-tamper-evident-wal-ledger.md); this note records *why* that work
exists and where it is meant to lead, so later sessions and contributors keep the thread.

## Origin

This direction comes from reviewing a separate project, VQP (Verifiable Query Protocol,
github.com/correodabid/vqp): a TypeScript library for answering predicates over private
data ("is age >= 18?") with a signed or zero-knowledge proof instead of revealing the data.

The review concluded:

- The idea is sound, but as a standalone, general-purpose protocol it competes with mature
  standards (W3C Verifiable Credentials, SD-JWT/BBS+, OpenID4VP, ISO mDL, the EU EUDI
  wallet) and ZK identity projects. A one-person protocol will not win adoption there.
- VQP's core flaw: a proof is only worth what its *anchor* is worth. A responder signing
  its own answer over its own data proves nothing. Valid anchors are an issuer that signs
  the data, a source the data provably came from (zkTLS), or attested execution (TEE).
- The natural place to create that anchor is **the database engine itself**: it sees every
  write, can commit cryptographically to the state at each LSN, and, if deterministic,
  lets an auditor, enclave or zkVM re-execute a query and get exactly the same result.
- ASQL already has the hard prerequisites: append-only WAL as source of truth,
  deterministic replay, time-travel by LSN, and a row-level audit store with old/new
  images and `commitLSN`.

So the decision was to **absorb VQP's goal into ASQL** as "verifiable queries" rather than
keep VQP as a separate product.

## Target niche

Regulated B2B audit and compliance, starting with pharma/GxP (21 CFR Part 11 audit trails,
batch release evidence), where today proof means on-site audits and PDFs, and companies
need to demonstrate compliance without opening their process data. Not consumer identity.

## Phases

1. **Tamper-evident WAL (ADR 0005).**
   - *Phase 1, shipped (#35):* hash chain over logical WAL records, chain anchors in
     segment headers and snapshots, `asqlctl verify-ledger`, `ledger=off|chain`
     (default `chain`). Acceptance test:
     `internal/engine/executor/ledger_tamper_test.go`. Before the ledger, editing a
     committed value in the WAL and fixing CRC32C replayed silently (`OUT_OF_SPEC`
     becomes `WITHIN_SPEC`). Now `verify-ledger` detects it and the store refuses to
     open.
   - *Known limit of phase 1:* every anchor lives in the same data directory. An
     attacker who can write there can recompute the whole chain, the segment anchors
     and the snapshots, and `verify-ledger` passes. Only a chain value exported
     beforehand and kept elsewhere exposes the rewrite
     (`internal/engine/executor/ledger_rewrite_test.go`). Phase 1b exists to close
     that gap.
   - *Phase 1b, shipped (#42):* `ledger=signed` with checkpointed Ed25519 digests, a
     digest sidecar log, `GET /api/v1/ledger/digest`,
     `SELECT * FROM asql_admin.ledger_digest` and `verify-ledger -digest`. Digests are
     meant to be published somewhere the operator does not control.
   - *Compliance profile (#49):* `asqld -compliance` refuses to start unless the
     ledger vouches for the whole retained WAL.
2. **Signed query results bound to state.** `SELECT ... AS OF LSN n` can return
   `{rows, lsn, chain/digest, signature}` so a third party can check that the answer comes
   from a specific, published state. Accepted in
   [ADR 0006](../adr/0006-signed-query-results.md): `ChainAt` and the attestation
   format (#60), `SELECT * FROM asql_admin.attest_query WHERE sql = '...'
   [AND lsn = N]` with `ledger=signed` (#61), and `asqlctl attest-query` and
   `verify-result`, which checks signature, rows, chain and re-execution apart.
3. **Inclusion and consistency proofs.** Move to an RFC 6962-style append-only Merkle tree
   (leaves are already position-independent for this). Prove "this row existed at state H"
   without revealing the rest, and "today's log extends yesterday's". The audit store's
   row images are the natural leaves, because WAL UPDATE/DELETE records are logical
   (predicates), not row images. Accepted in
   [ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md): an RFC 6962 log tree
   over the ledger leaves first, then a salted sparse Merkle tree of the rows of
   `VERIFIABLE` tables, both built off the commit path and signed in digest v2.
   - *Log tree (#86), signed in digest v2:* each checkpoint extends the log tree and
     signs its root at the digest's LSN (`tree_root` in `asql_admin.ledger_digest` and
     `GET /api/v1/ledger/digest`). `verify-ledger -digest` rebuilds the tree from the
     WAL and checks it. WAL GC never removes records the tree has not added.
   - *Consistency proofs:* `asql_admin.prove_consistency`, `asqlctl prove-consistency`
     and `verify-consistency`, which checks a proof between two signed digests with the
     trusted keys alone.
   - *State tree and row proofs:* `VERIFIABLE` tables (with a primary key, composite
     ones included), digest v3 signing the state root, and batch row proofs:
     `asql_admin.prove_rows`, `asqlctl prove-rows` and `verify-rows`. Next: group leaves,
     to prove that a report lists all the parents of an artifact.
4. **Verifiable computation.** Aggregates over private data executed in a TEE (e.g. AWS
   Nitro Enclaves) with attestation, and ZK only for narrow predicates where it pays off.

## Standing constraints

- The integrity of history is not the truth of the data. Source authenticity (signed
  inserts from instruments or upstream systems, recorded as commit metadata) is a
  separate, later concern.
- Everything must respect the determinism contract in `AGENTS.md`. Ledger data is
  metadata about the log, never engine-observable state.
- Performance is a hard requirement. Measured cost, guardrails and CI gates are in
  ADR 0005 § Performance. Signing per commit is ruled out by those numbers.
- Integrate with standards (DIDs, Verifiable Credentials, OpenID4VP) at the edges rather
  than reinventing them.
