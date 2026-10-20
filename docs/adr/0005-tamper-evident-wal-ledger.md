# ADR 0005: Make the WAL tamper-evident with a hash-chained ledger and signed digests

- Status: Accepted. Phase 1 (`ledger=off|chain`, anchors, `verify-ledger`) shipped in #35. Phase 1b (`ledger=signed`, digests and their surfaces) shipped in #42. The compliance profile is #49. Signed query results, which build on these digests, are proposed in [ADR 0006](0006-signed-query-results.md).
- Date: 2026-10-14
- Decision drivers:
  - ASQL positions itself as a database teams can reason about under audit; today the WAL detects accidental corruption but not deliberate rewriting
  - regulated workloads (e.g. GxP / 21 CFR Part 11 audit trails) need evidence that history was not altered after the fact
  - the mechanism must preserve the determinism contract (AGENTS.md rules 1-6) and must not regress the commit path
  - it must survive the on-disk transformations ASQL already performs (s2 payload compression, WALZ segment compaction, v4 to v5 record migration, WAL GC)
  - it should be the foundation for later verifiable-query work (inclusion proofs, signed query results), without committing to that work now

## Context

Each WAL record today is framed as:

```
[len][version | LSN | Term | Timestamp | TxIDLen | TxID | Type | s2(Payload) | CRC32C]
```

(see [internal/storage/wal/binary.go](../../internal/storage/wal/binary.go)).

CRC32C is an integrity check against torn writes and bit rot. It is not a security
mechanism: anyone with write access to the data directory can change a payload,
recompute the CRC, and the engine will replay the altered history without complaint.
Segment headers are protected the same way (CRC32 over bytes `[0:60]`).

Facts about the current code that constrain the design:

1. **On-disk bytes are not stable.** Mutation payloads are s2-compressed per record, and
   `compactSegment` later rewrites sealed segments into whole-segment zstd (WALZ).
   Legacy v4 records decode with `Term=0`. Any hash over file bytes would break on
   compaction or migration.
2. **There are several append paths.** `Append`, `AppendBatch`, `AppendBatchNoSync`,
   `AppendReplicated` and `AppendReplicatedBatch` on `SegmentedLogStore`, plus the legacy
   `FileLogStore`. Followers write through the replicated paths with leader-assigned LSNs.
3. **Commits are serialized.** `commitQueue.writerLoop` builds `BEGIN + MUTATION* + COMMIT`
   records for a coalesced group of jobs and issues one WAL write per group. Anything
   added to that path is on the critical section for every writer.
4. **UPDATE and DELETE are logged logically** (columns + predicate, see
   `mutation_payload_v2.go`), not as row images. A WAL hash therefore certifies the
   *inputs*; the state at a given LSN is obtained by deterministic replay. Row images
   with `commitLSN` live in the separate audit store (`internal/storage/audit`), which is
   also a `SegmentedLogStore`.
5. **WAL GC is optional.** With `WithWALGC()`, segments below the latest snapshot LSN are
   removed, so the beginning of the log can disappear.

## Decision

ASQL will maintain a cryptographic hash chain over the logical content of every WAL
record, persist chain checkpoints alongside segments and snapshots, and expose
Ed25519-signed digests that third parties can retain and later verify against.

### 1. Hash construction

Hashes are computed over **logical fields with the uncompressed payload**, never over
on-disk bytes. All integers are big-endian, matching the existing record encoding.

```
leaf_n  = SHA-256( 0x00 || Term || Timestamp || u16(len TxID) || TxID
                        || TypeEnum || u32(len Payload) || Payload )

chain_n = SHA-256( 0x01 || chain_{n-1} || LSN_n || leaf_n )

chain_0 = SHA-256( 0x02 || "asql-ledger-v1" )
```

- The genesis takes no parameters: every replica and data directory shares
  `chain_0`. A future format can bind deployment parameters under a new domain
  string (`asql-ledger-v2`).

- Domain-separation prefixes (`0x00`, `0x01`, `0x02`) prevent a leaf from being
  reinterpreted as a chain node, the same discipline RFC 6962 uses.
- The LSN goes into the chain step, not the leaf. Leaves are then position-independent,
  so they can be computed before LSN assignment and outside the WAL lock (see
  Performance), and they can later become leaves of a Merkle tree unchanged.
- `TypeEnum` is the existing 1-byte enum, so the encoding does not depend on type
  strings.
- SHA-256 is chosen over BLAKE3 or others because it is FIPS-approved and
  hardware-accelerated on ARMv8 (Apple Silicon, Graviton) and on x86 with SHA-NI. Audit
  and compliance reviewers expect it.

### 2. Where the chain is computed

A single helper in `internal/storage/wal` advances the chain. Every append path calls
it after LSNs are assigned and before `lastLSN` is published. The replicated paths
compute the chain from the leader-assigned LSNs. Because the inputs are identical, every
replica derives the same `chain_n`. That gives a divergence check between nodes that
does not exist today: followers can compare their chain head against the leader's.

The chain is a pure function of the WAL contents. It reads no clock, randomness, or
process state, so it is compatible with determinism rules 1-3 by construction. It is
**metadata about the log**, not engine state. Queries do not observe it, and replay
does not depend on it.

### 3. Persistence

- **Record format is unchanged.** No new bytes per record, no new WAL record type.
- **Segment header v4.** The current header is v3 (64 bytes) and its 22 reserved
  bytes cannot hold two 32-byte hashes, so the header grows to v4 (128 bytes): the v3
  fields at the same offsets, a ledger flags byte, `chainBeforeFirstLSN` and
  `chainAtLastLSN`, still covered by the header checksum. The data offset follows the
  header version, and v3 headers stay readable. With `ledger=off` new segments keep
  v3 headers. Segments without chain data are treated as "unanchored" history; their
  records are still covered by the recomputed chain.
- **Snapshots store `chain_at_snapLSN`.** This is what keeps the chain verifiable when
  WAL GC removes earlier segments: the snapshot becomes the anchor for the retained
  suffix.
- **Startup.** The chain head is restored from the last sealed segment header, and only
  the active segment is rehashed. On sealed segments, the header value is trusted for
  startup and fully checked by `verify-ledger` (below), so restart time does not grow
  with history size. If a rehashed segment no longer matches its own header anchors,
  the store refuses to open (`ErrLedgerMismatch`) instead of re-anchoring over the
  modified records, the same fail-closed behavior as a CRC failure.

### 4. Signed digests

A digest is a signed statement that the chain had value `chain` at a committed `lsn`:

```
{ version: 1, lsn, chain, term, timestamp, node_id, key_id, signed_at,
  sig: Ed25519(canonical bytes) }
```

- **Only committed state is signed.** The signed LSN is the engine's applied head.
  The engine applies only entries Raft has committed, so a digest never covers a WAL
  tail that a new leader could still replace (#37, #38). Signing the WAL's last LSN
  instead would produce valid, signed digests that contradict the legitimate log,
  which looks exactly like tampering.
- **Canonical bytes, not JSON.** The signature covers a fixed big-endian encoding:
  the domain string `asql-ledger-digest-v1`, a zero byte, then `version`, `lsn`,
  `chain`, `term`, `timestamp`, `signed_at` (Unix ns), and length-prefixed `node_id`
  and `key_id`. JSON is only the transport form; JSON canonicalization is a classic
  source of signature bugs.
- **Fields.** `term` and `timestamp` belong to the WAL record at `lsn`. `timestamp`
  is the logical WAL timestamp, not wall-clock time. `signed_at` is the signer's
  wall clock: it is signed, but only the node asserts it. Proving that a state
  existed by a given time requires publishing or time-stamping the digest elsewhere
  (RFC 3161, a transparency log).
- **Checkpointed, never per commit.** A checkpointer signs at most every second, or
  sooner after 10,000 committed LSNs, and only when the head has advanced. Both
  values are configurable. It runs off the commit path. Shutdown signs the final
  head. Ed25519 is deterministic, and signatures live outside engine-observable
  state.
- **Keys.** Each node has its own key. Every replica holds the same chain, so
  digests from several nodes at one LSN act as independent witnesses. Keys are
  standard PEM files: PKCS #8 private keys, mode 0600, which `openssl genpkey
  -algorithm ed25519` or `asqlctl -command ledger-keygen` produce, and PKIX public
  keys. `key_id` is the first 8 bytes of SHA-256 over the public key, in hex. Signing
  goes through `ports.LedgerSigner`, so KMS or HSM backends can be added later.
- **Verification needs a trusted key from outside.** A digest never carries the key
  that verifies it; otherwise an attacker would re-sign a forged digest with their
  own key. Verifiers pass trusted public keys explicitly.
- **Sidecar log.** Digests are appended, one JSON line each and synced, to
  `<data-dir>/ledger/digests-<first LSN>.jsonl`. Files rotate at 16 MiB and are kept,
  because digests are evidence. At the defaults, continuous writes produce at most
  one digest per second, about 17 MB per day. A torn last line is truncated on open.
- **Publishing.** Publishing digests somewhere the operator does not control is what
  turns "tamper-evident" into "tamper-proof in practice" (see Threat model).
  Examples: an auditor's mailbox, object storage with object lock, a transparency
  log. That step is deliberately left to deployments.

### 5. Surfaces

- Admin HTTP: `GET /api/v1/ledger/digest` returns the latest digest, and
  `GET /api/v1/ledger/digest?lsn=N` returns the oldest digest covering LSN N. That is
  what an auditor needs to check a record at N; arbitrary LSNs are not signed on
  demand. Read scope, like the other `/api/v1` endpoints.
- SQL: `SELECT * FROM asql_admin.ledger_digest [WHERE lsn >= N]`, the same as one
  row, for clients that only speak pgwire. It follows the existing `asql_admin.*`
  virtual tables and is served by the pgwire catalog layer, outside the engine. Node-
  specific digest data therefore cannot reach the WAL, for example through
  `INSERT ... SELECT`, and break replay determinism.
- CLI: `asqlctl -command verify-ledger -data-dir D [-digest d.json -pubkey keys.pem]
  [-from-snapshot]`. It reads segments (raw and WALZ), recomputes leaves and the
  chain, and checks segment header and snapshot anchors. With `-digest` it also
  checks the digest's signature against the trusted keys in `-pubkey` (one or more
  PEM public keys) and its chain value against the recomputed chain. It fails when
  the WAL ends before the digest's LSN, because committed records were removed, and
  when the retained WAL starts after it.
  Exit code non-zero on mismatch. It reports the first divergence as an interval
  `(last good anchor, first failing anchor]`. A break in the LSN sequence (a dropped,
  duplicated or reordered record) is reported at the exact LSN. A modified record is
  located only as precisely as the anchors allow: segment headers, snapshots and
  digests are checkpoints, not per-record values. Exact per-record location needs a
  reference sequence (a replica or a copy).

### 6. Configuration

- `ledger` mode with values `off | chain | signed` (`asqld -ledger`). Default `chain`,
  because it is cheap (see Performance) and makes every deployment tamper-evident
  without configuration. `signed` maintains the chain exactly like `chain` and needs
  `-ledger-key`. `-ledger-digest-interval` and `-ledger-digest-every` tune the
  checkpoints.
- A compliance profile refuses to start with `WithWALGC()` unless segments are archived
  before removal, and refuses to start if any retained segment is unanchored. Not
  implemented yet.

## Threat model and current limit

The chain detects a modification only when something the attacker cannot rewrite
still holds an earlier chain value. In phase 1 every anchor (segment headers,
snapshots) lives in the same data directory as the WAL. Against an attacker with
write access to that directory, phase 1 therefore guarantees less than its name
suggests:

- **Detected:** editing, dropping, reordering or truncating records while leaving
  the anchors alone, which is what a careless or partial edit does. Also detected
  is any accidental divergence between replicas.
- **Not detected:** a full rewrite. The attacker edits a value, recomputes the
  record CRCs and the whole chain from the edit onwards, rewrites the segment header
  anchors, and deletes the snapshots so the engine regenerates them with matching
  anchors. Everything on disk is then self-consistent and `verify-ledger` passes.
  The chain construction is public and keyless, so nothing on disk can prevent this.

`internal/engine/executor/ledger_rewrite_test.go` demonstrates both halves. After a
full rewrite `verify-ledger` passes, while verification against a chain value
exported before the rewrite (`VerifyOptions.Anchors`) fails with `chain-mismatch`.

Closing the gap needs a chain value the attacker cannot rewrite:

1. **Signed digests (phase 1b).** A signature stops a forged digest from being
   minted without the node key. It does not stop an attacker who also holds the key,
   such as one with root on the node. `verify-ledger -digest` fails on a replaced but
   self-consistent history and on a clean truncation, both of which pass without a
   digest (`cmd/asqlctl/ledger_digest_command_test.go`).
2. **Digests held elsewhere.** Digests must be published somewhere the operator of
   the data directory does not control: an auditor, object storage with object lock,
   or a transparency log. Then a rewrite is detected against any digest published
   before it. This is the guarantee regulated deployments need, and it comes from
   deployment practice, not from the database alone.

Known gap: a node that started without a persisted Raft commit index (an upgrade
from before #38) assumes its whole WAL is committed. If the new leader later replaces
part of it, the engine rolls back (`RollbackTo`), but digests it signed in between
remain in the log and contradict the new history. No attacker is involved, only
that upgrade path, and the other nodes' digests are unaffected.

## Performance

The requirement is: **no measurable regression on the commit path, and bounded,
documented overhead in the worst case.**

### Implementation (phase 1)

Measured on 2026-10-15 against `d78f6dc` (phase 1 merged, plus the Raft fixes that
followed). Environment: Apple M5, darwin/arm64, 10 cores, hardware SHA-256, run
natively. The no-SHA-NI x86 case was not re-measured; the prototype figures below
remain the reference for it.

Method: `-count=10`, interleaving `off` and `chain` runs and alternating which mode
goes first in each round, compared with benchstat. The engine benchmarks select the
mode with `ASQL_BENCH_LEDGER=off|chain`.

**Primitives** (`BenchmarkLedgerLeaf`, `BenchmarkLedgerChainStep`, n=6; Ed25519 with
`crypto/ed25519` over a 128-byte message). All are allocation-free:

| Operation | Cost |
|---|---|
| Leaf, empty payload (BEGIN/COMMIT) | 47 ns |
| Leaf, 256 B payload | 119 ns |
| Leaf, 1 KiB payload | 356 ns |
| Leaf, 4 KiB payload | 1.30 µs |
| Chain step | 64 ns |
| Ed25519 sign | 11.6 µs |

**WAL append of one transaction, no fsync** (`BenchmarkSegmentedAppendTxNoSync`):

| Mutation size | off | chain | Overhead per tx | allocs/op off → chain |
|---|---|---|---|---|
| 128 B | 2.12 µs ± 3% | 2.59 µs ± 3% | +0.47 µs (+22%, p=0.000) | 9 → 9 |
| 1 KiB | 2.24 µs ± 1% | 2.97 µs ± 1% | +0.73 µs (+33%, p=0.000) | 9 → 9 |

The primary gate holds: at most 1 µs per transaction, and no extra allocations.

**End-to-end engine commit**:

| Benchmark | off | chain | Difference |
|---|---|---|---|
| `EngineWriteCommit` | 734 µs ± 6% | 744 µs ± 4% | ~ (p=0.280) |
| `EngineWriteCommitConcurrent` | 238 µs ± 0% | 238 µs ± 1% | ~ (p=0.739) |

benchstat finds no significant difference, so the secondary gate holds. Bytes and
allocations per op do not change either.

**`ledger=signed`** does not touch the commit path. The checkpointer costs one
Ed25519 signature (11.6 µs), one WAL read from the recent-record cache and one synced
append to the digest log, at most once per second by default.

### Prototype (reference)

Measurements were taken on 2026-10-14 against `71b1a8f` with a throwaway prototype. The
leaf and chain were computed inside the existing `SegmentedLogStore` append paths, under
the WAL lock. That is the naive placement, so the numbers are an upper bound on cost.
They were run on two environments:

- **ARM64:** Apple Silicon MacBook Air, Linux arm64 VM, 4 vCPU, hardware SHA-256 (ARMv8
  SHA2 instructions)
- **x86 (no SHA-NI):** cloud Intel Xeon 2.8 GHz, 2 vCPU, *software* SHA-256. This is the
  pessimistic case.

The prototype leaf included the LSN. The construction above moves the LSN into the
chain step, which hashes the same number of SHA-256 blocks, so the cost is equivalent.

**Primitive costs** (median of 3 runs):

| Operation | ARM64 | x86 (no SHA-NI) |
|---|---|---|
| Leaf + chain, empty payload (BEGIN/COMMIT records) | 107 ns | 840 ns |
| Leaf + chain, 256 B payload | 181 ns | 1.8 µs |
| Leaf + chain, 1 KiB payload | 413 ns | 4.2 µs |
| Leaf + chain, 4 KiB payload | 1.34 µs | 14–16 µs |
| Chain step only (leaf precomputed) | 60 ns | 500 ns |
| Ed25519 sign of chain head | 15.4 µs | 31–37 µs |

**WAL append of one transaction (BEGIN + 1 MUTATION + COMMIT), no fsync.** This
benchmark is pure CPU plus page cache, so the relative overhead is at its worst here.
Medians are over interleaved runs (ARM64 n=5, x86 n=8):

| Mutation size | Mode | ARM64 | x86 (no SHA-NI) |
|---|---|---|---|
| 128 B | off | 1.78 µs | 5.3 µs |
| 128 B | chain | 2.20 µs (+0.4 µs) | 10.4 µs (+5.1 µs) |
| 128 B | chain + sign every append | 18.4 µs | 41.8 µs |
| 1 KiB | off | 2.23 µs | 5.3 µs |
| 1 KiB | chain | 2.92 µs (+0.7 µs) | 12.4 µs (+7.1 µs) |
| 1 KiB | chain + sign every append | 18.6 µs | 46.5 µs |

**End-to-end engine commit** (`BenchmarkEngineWriteCommit*`, 3000 iterations,
interleaved, n=8, x86 cloud; this path is dominated by durable I/O):

| Benchmark | off | chain | chain + sign every append |
|---|---|---|---|
| `EngineWriteCommit` | 1086 µs | 1102 µs (+1.5%) | 1121 µs (+3.3%) |
| `EngineWriteCommitConcurrent` | 939 µs | 955 µs (+1.7%) | 985 µs (+4.8%) |

Run-to-run spread for these was 3–7%. The chain-only difference (+1.5–1.7%) is
therefore **inside the noise floor**, even with software SHA-256 and the naive
placement.

What the numbers say:

1. **The hash chain is negligible for real commits.** It costs about 0.4–0.7 µs per
   transaction on hardware-accelerated SHA, against commit latencies dominated by fsync
   (tens of µs to ms). Even in the pessimistic software-SHA case it stays below the noise
   of the end-to-end benchmarks.
2. **It is visible only on the raw, no-fsync append path.** There it is +24–31% on
   ARM64, and roughly 2x with software SHA. That path only exists for non-durable
   ingestion, and the design rules below bring it down further.
3. **Signing every commit is not acceptable** as a default: +15–35 µs per append and a
   measurable end-to-end hit (+3–5%). Signing must be checkpointed. That decision is
   driven by data, not taste.
4. The prototype added 3 small allocations per append (hasher `Sum` escaping). The real
   implementation must be allocation-free: use `sha256.Sum256` on a reused buffer, or a
   concrete hasher.

### Design rules that keep it cheap

1. **Leaves are hashed outside the WAL lock.** Because the leaf excludes the LSN, every
   append path computes its leaves on entry, before taking the store lock; under the
   lock only the chain step runs: one 2-block SHA-256 per record, about 60 ns on ARM64.
   The leaves cannot be computed earlier, in the committing session: the leaf covers
   `Timestamp`, which `commitQueue.writerLoop` assigns under `writeMu` (and it may
   re-encode `job.preEncoded` there), and `Term`, which Raft assigns. In standalone
   mode the leaf cost therefore stays on the serialized writer loop, outside the WAL
   lock.
2. **No extra allocations on the hot path.** Leaves up to 256 bytes hash a stack
   buffer. Larger leaves, the hasher and the batch leaf slice use one spare buffer
   per store, held in an `atomic.Pointer`. A `sync.Pool` was tried first, but GC
   empties it, which showed up as fractional allocations per append. The chain
   step hashes a fixed-size stack array.
3. **No extra I/O per commit.** Chain values reach disk only through segment headers
   (written when headers are already persisted) and through snapshots.
4. **Signing is amortized.** At most one signature per group commit, by default far
   fewer (checkpointed).
5. **Kill switch.** `ledger=off` restores exactly today's path, for benchmarking and
   emergencies.

### Guardrails

- New benchmarks: `BenchmarkLedgerLeaf`, `BenchmarkLedgerChainStep`,
  `BenchmarkSegmentedAppendTxNoSync` (ledger on/off) and the existing
  `BenchmarkEngineWriteCommit*` set run with `ledger=off|chain`. `signed` adds
  nothing to the commit path, so it has no commit benchmark of its own.
- CI performance gates. The end-to-end commit benchmarks have 3–7% run-to-run noise on
  shared runners, so a tight percentage gate on them alone would be flaky:
  - **Primary, CPU-bound and stable:** on `BenchmarkSegmentedAppendTxNoSync`, the chain
    overhead per transaction must stay at or below 1 µs on the ARM64 runner, and
    allocations per append must not increase versus `ledger=off`.
  - **Secondary, end-to-end:** `BenchmarkEngineWriteCommit` and
    `BenchmarkEngineWriteCommitConcurrent` with `ledger=chain` must not be slower than
    `ledger=off` by more than 3%, with statistical significance (benchstat,
    `-count=10`, p < 0.05).
- Restart time: `BenchmarkEngineRestart*` must not regress, because only the active
  segment is rehashed.

## Testing

- **Tamper test (the point of this ADR).** Write transactions, edit a mutation
  payload on disk and recompute the record CRC32C. Before this ADR the engine
  replayed it silently. Now `verify-ledger` fails and locates the edit within the
  interval between anchors (see Surfaces), and the store refuses to open.
- **Full-rewrite test.** Shows the limit described in Threat model.
- Reorder, drop, and truncate-and-append variants of the tamper test.
- Compaction invariance: the chain is identical before and after `CompactSealedSegments`.
- v4 to v5 migration invariance.
- Replication: leader and followers produce identical chain heads after mixed
  `Append` and `AppendReplicatedBatch` traffic; a diverged follower is detected.
- WAL GC: after truncation, verification from the snapshot anchor succeeds, and
  verification that claims to start from genesis fails explicitly.
- Determinism suite (`./test/determinism/...`) unchanged and green with `ledger=chain`.

## Consequences

Positive:

- Every ASQL deployment becomes tamper-evident by default, with negligible cost.
- Replicas gain a cheap divergence check.
- Signed digests give auditors something concrete to retain and verify independently,
  which supports the audit/compliance positioning directly.
- Leaves are already in the shape needed for an RFC 6962-style Merkle tree, so
  inclusion and consistency proofs can come later without changing the hash format.

Negative / costs:

- Segment header format change (v3 to v4, 64 to 128 bytes) and snapshot format change.
- Key management becomes an operator concern in `signed` mode.
- The chain proves the log was not rewritten, **not that the data was true when it was
  written**. Source authenticity (signed inserts from instruments or upstream systems)
  is a separate concern.

## Non-goals (now)

- Merkle inclusion and consistency proofs (O(log n) proofs per row or per prefix).
  Planned as follow-up, probably over the audit store's row images rather than the
  logical WAL.
- Signed query results (`SELECT ... AS OF LSN n` returning `{rows, lsn, chain, sig}`).
- Zero-knowledge or TEE-based verifiable computation.
- Chaining the audit store. It uses the same `SegmentedLogStore`, so it inherits the
  mechanism almost for free, but enabling it is a separate decision.

## Alternatives considered

- **Hash the on-disk frames.** Rejected: breaks on s2 or WALZ recompression and on
  record-version migration.
- **Replace CRC32C with a per-record HMAC.** Rejected: detects edits but not deletion
  or reordering of whole records, and needs a secret on every verifier.
- **Merkle tree from day one.** Deferred: more code and state for proofs nobody consumes
  yet. The leaf format is chosen so the upgrade is additive.
- **External ledger database (immudb, QLDB-style service).** Rejected: a side channel
  around the WAL, contrary to AGENTS.md ("the WAL is truth"), and an extra operational
  dependency.
