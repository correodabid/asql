# 13. Verifiable History

ASQL can prove what its history was to someone who does not trust the node: a
regulator, a reviewer, the reader of an exported report. This guide goes from a
signed ledger to a report whose rows, and whose lineage, anyone can check with a
public key.

:::info[What is proven, and what is not]
Every proof here shows what the node **recorded and signed**: which rows it held at a
given point, which result a query gave, that today's log extends yesterday's. None of
it proves that the data is *true*: that is still the application's and its sources'
responsibility. See the "What this proves" section of
[ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md).
:::

## The layers

| Layer | What it gives | Needs |
|---|---|---|
| Hash chain (default) | Every WAL record is chained; `verify-ledger` detects any edit, removal or reordering | nothing: `-ledger chain` is the default |
| Signed digests | The node signs the chain head at checkpoints; a digest kept elsewhere exposes a rewritten history | `-ledger signed` and a signing key |
| Signed query results | A SELECT's rows with a signature tied to the chain at a committed LSN | signed ledger |
| Consistency proofs | Today's signed log extends yesterday's, without seeing any record | signed ledger |
| Row proofs | Rows of `VERIFIABLE` tables, cited by primary key, proven in one batch without revealing other rows | signed ledger and a state key |
| Complete groups | "These are *all* the parents of this artifact", none for a source | a verifiable group |

## 1. Create the node's keys

:::steps
1. **Generate the key pair and the state key**

   ```bash
   go run ./cmd/asqlctl -command ledger-keygen -output /etc/asql/node.key
   # node.key        Ed25519 private key (0600): signs digests and results
   # node.pub        its public key: give it to whoever verifies
   # node.key.state  state key (0600): salts the row proofs
   ```

2. **Keep the private key and the state key secret.** Every node of a cluster needs
   the same two files. Verifiers only ever need `node.pub`.
:::

For a node whose key predates the state key, run the same command on the existing
`node.key`: it adds only `node.key.state`.

## 2. Run with a signed ledger

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql -admin-addr :9090 \
  -ledger signed -ledger-key /etc/asql/node.key
```

The node signs the committed head at most every second, or every 10,000 LSNs
(`-ledger-digest-interval`, `-ledger-digest-every`), off the commit path. It loads
`node.key.state` on its own; `-ledger-state-key` names another file.

Read the latest digest over SQL or HTTP:

```sql
SELECT version, lsn, chain, tree_root, state_root, signed_at FROM asql_admin.ledger_digest;
```

```bash
curl -s http://127.0.0.1:9090/api/v1/ledger/digest > digest.json
```

**Publish digests somewhere the operator does not control** (a ticket, an email to the
auditor, a transparency log). A digest kept elsewhere is what makes a rewritten history
detectable.

## 3. Check a data directory against a digest

```bash
go run ./cmd/asqlctl -command verify-ledger -data-dir /copy/of/.asql \
  -digest digest.json -pubkey node.pub
```

It recomputes the chain and the log tree from the WAL and checks them against the
signed digest. A WAL edited, truncated or replaced by another consistent history fails.

## 4. Sign a query's result

```bash
go run ./cmd/asqlctl -command attest-query -pgwire 127.0.0.1:5433 \
  -sql "SELECT id, result FROM lab.results ORDER BY id" -output result.json

go run ./cmd/asqlctl -command verify-result -result result.json -pubkey node.pub \
  [-digest digest.json] [-data-dir /copy/of/.asql]
```

`verify-result` checks the signature and the rows with the key alone; with a digest it
ties the result to the published history, and with a data directory it re-executes the
query.

## 5. Prove that the log only grew

Keep yesterday's digest. Today, ask the node for a proof from it to the latest one:

```bash
go run ./cmd/asqlctl -command prove-consistency -pgwire 127.0.0.1:5433 \
  -from-lsn <LSN of yesterday's digest> -output proof.json
go run ./cmd/asqlctl -command verify-consistency -proof proof.json -pubkey node.pub
```

Compare the proof's `from_digest` with the copy you kept: a node that rewrote its
history after it cannot produce a valid proof to it.

## 6. Make tables verifiable

Rows are proven by primary key, so a verifiable table needs one; composite keys work.

```sql
BEGIN DOMAIN lab;
CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT, payload JSON) VERIFIABLE;
CREATE TABLE artifact_parents (
  child_id TEXT, parent_id TEXT, ordinal INT,
  PRIMARY KEY (child_id, parent_id)
) APPEND ONLY VERIFIABLE;
-- Every artifact gets a group: all of its parent edges, none for a source.
ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id);
COMMIT;
```

For existing tables, `ALTER TABLE … ADD PRIMARY KEY (…)` and `ALTER TABLE … SET
VERIFIABLE` do the same. `VERIFIABLE` restricts nothing, and none of these can be
undone. From the next checkpoint on, every digest is version 3 and signs `state_root`.

## 7. Prove the rows a report cites

List the rows the report cites, and the groups it claims complete:

```json
[{"domain": "lab", "table": "artifacts", "pk": ["0192…"]},
 {"domain": "lab", "table": "artifact_parents", "group": ["0192…"]}]
```

```bash
go run ./cmd/asqlctl -command prove-rows -pgwire 127.0.0.1:5433 \
  -keys lineage.json -output proof.json            # add -lsn N for an older digest
go run ./cmd/asqlctl -command verify-rows -proof proof.json -pubkey node.pub
```

`verify-rows` needs only the public key. It lists what it checked, for example
`lab.artifacts(id=0192…)` and `lab.artifact_parents(child_id=0192…): all 2 rows`.
Changing any value, leaving a row of a group out, or presenting an artifact with
parents as a source makes it fail.

:::details[What a row proof reveals]
The cited rows, the digest's LSN, and roughly the order of magnitude of the number of
rows in the verifiable tables. Nothing about the content or the existence of any other
row: the other leaves are salted with the node's secret state key. It cannot prove
that a row is absent, and groups prove completeness only in the direction they were
declared (an artifact's parents, never its children).
:::

## Where to go next

- [../reference/database-security-model-v1.md](../reference/database-security-model-v1.md):
  every surface above, with its privileges and failure modes.
- [ADR 0005](../adr/0005-tamper-evident-wal-ledger.md),
  [ADR 0006](../adr/0006-signed-query-results.md) and
  [ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md): the formats and why.
- [../architecture/verifiable-queries-roadmap-v1.md](../architecture/verifiable-queries-roadmap-v1.md):
  what is shipped and what comes next.
