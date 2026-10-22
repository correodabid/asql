# Database Security Model v1

## Scope

This document defines the current ASQL database security model for durable
principals, pgwire identity, temporal access, and practical lifecycle
operations.

Policy stance:

- transport/admin tokens remain deployment and operator controls,
- durable principals are the database identity and authorization model,
- historical reads are explicitly privileged and audited,
- security state is durable engine state and survives restart/replay.

For the architectural decision behind this model, see
[../adr/0004-durable-database-principals-and-historical-authorization.md](../adr/0004-durable-database-principals-and-historical-authorization.md).

## Keep the two security layers separate

ASQL exposes two different security layers on purpose.

### 1. Operator/process controls

These are configured at process startup:

- `-auth-token`
- `-admin-read-token`
- `-admin-write-token`

Use them for:

- protecting pgwire before durable principals are bootstrapped,
- protecting admin HTTP and other operator-sensitive surfaces,
- deployment-time and local-operator control.

These tokens are not the durable database identity model.

### 2. Durable database principals

These are engine-owned and replay-safe:

- `USER`
- `ROLE`
- role membership
- principal state such as enabled/disabled
- privilege grants and revokes

Use them for:

- pgwire database login once the principal catalog exists,
- current-state authorization,
- temporal authorization,
- effective-permission inspection and auditability.

## Bootstrap semantics

The first admin principal is special.

Bootstrap is allowed only while the durable principal catalog is empty.
After the first admin exists, steady-state principal management should happen
through the durable model rather than through process configuration.

That means:

- operator tokens can protect the bootstrap path,
- the bootstrap path is a one-time catalog initialization step,
- later user/role/password changes should use Studio, `asqlctl`, or the admin
  API against the durable principal catalog.

### CLI bootstrap example

```bash
printf 'admin-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token write-secret \
	-password-stdin \
	-command principal-bootstrap-admin \
	-principal admin
```

### Studio bootstrap example

If Studio connects to an ASQL node whose durable principal catalog is still
empty, the `Security` area exposes a dedicated bootstrap form for the first
admin principal.

## Current durable-principal privilege surface

The current documented MVP surface is intentionally narrow.

| Capability | Current rule |
|---|---|
| Current-state `SELECT` | Any authenticated enabled principal |
| Current DDL/DML/schema-changing statements | `ADMIN` |
| Operator/admin virtual helpers under `asql_admin.*` | `ADMIN`, except historical helpers |
| Historical reads: `AS OF LSN`, `AS OF TIMESTAMP`, `FOR HISTORY` | `SELECT_HISTORY` |
| Historical helper views such as `asql_admin.row_history` and `asql_admin.entity_version_history` | `SELECT_HISTORY` |
| `asql_admin.attest_query` (signed query results, ADR 0006) | the privileges of the query it attests, plus `SELECT_HISTORY` at an LSN other than the head |

This is not full PostgreSQL role/privilege parity.
It is the current ASQL-documented security surface.

## Append-only tables

Some tables must never change once a row is written, for example evidence or published
findings. Privileges cannot guarantee that, because any `ADMIN` can run DML. Instead the
table itself is made append-only:

```sql
CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT, meta JSON) APPEND ONLY;
-- or, for an existing table:
ALTER TABLE artifacts SET APPEND ONLY;
```

An append-only table accepts `INSERT`, `INSERT ... ON CONFLICT DO NOTHING` and
`ALTER TABLE ... ADD COLUMN`. The engine rejects the following with SQLSTATE `42501`, for
every principal, `ADMIN` included:

- `UPDATE`
- `DELETE`
- `TRUNCATE TABLE`
- `DROP TABLE`
- `ALTER TABLE ... DROP COLUMN`
- `INSERT ... ON CONFLICT DO UPDATE`

How the guarantee holds:

- The violation is reported on the statement. It is checked again when the transaction
  commits, so a change queued before the table became append-only is still rejected.
- The setting cannot be undone: there is no statement that clears it.
- The setting is recorded in the WAL like any DDL, so the ledger (ADR 0005) covers it,
  and it survives replay and snapshots.
- `SchemaSnapshot` and `asql_admin.schema_snapshot` report it as `append_only`.

## Verifiable tables

A table marked `VERIFIABLE` will have its rows entered in the ledger's state tree
([ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md)), so that a node can prove
one row's value at a signed checkpoint without revealing any other row:

```sql
CREATE TABLE results (id INT PRIMARY KEY, result TEXT) VERIFIABLE;
CREATE TABLE evidence (id TEXT PRIMARY KEY, sha256 TEXT) APPEND ONLY VERIFIABLE;
-- or, for an existing table:
ALTER TABLE results SET VERIFIABLE;
```

- It needs a primary key, since rows are proven by primary key: a table without one
  fails with `42P16`.
- It restricts nothing: a verifiable table accepts the same statements as before.
  Combine it with `APPEND ONLY` for rows that must also never change.
- It cannot be undone; setting it again is a no-op. Like other `ALTER TABLE`
  statements it needs `ADMIN`.
- The WAL and snapshots record it, and `asql_admin.schema_snapshot` reports it as
  `verifiable`.
- Marking an existing table enters its live rows at that LSN; proofs start at the first
  signed checkpoint after it.

With `ledger=signed` and a state key, every digest is version 3: besides the chain and
the log tree root it signs `state_root`, the root of the state tree of the verifiable
tables' live rows at its LSN (`asql_admin.ledger_digest` and `GET /api/v1/ledger/digest`
show it). The node keeps the tree off the commit path. `verify-ledger -digest` reports
`state_root` as `not-checked`: rebuilding it needs the secret state key. The row proofs
against it (`asql_admin.prove_rows`, `asqlctl verify-rows`) come next.

## Ledger keys

`ledger=signed` needs a signing key; `asqlctl ledger-keygen` writes it with the files
that go with it, and never overwrites one:

```bash
asqlctl -command ledger-keygen -output /etc/asql/node.key
# node.key        Ed25519 private key that signs digests and attestations (0600)
# node.pub        its public key, for verifiers
# node.key.state  state key (0600)
asqld -ledger signed -ledger-key /etc/asql/node.key [-ledger-state-key /other/path]
```

The **state key** is a 32-byte secret, separate from the signing key, that will salt
the leaves of the state tree of `VERIFIABLE` tables
([ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md)), so a proof about one row
does not let anyone test guesses about its neighbours. It is a PEM block of type
`ASQL LEDGER STATE KEY`.

- `asqld` loads it from `<ledger-key>.state` unless `-ledger-state-key` names
  another file (`LedgerStateKeyFile` in `pkg/embed`). A file named with the flag, or a
  default file that exists, must load: wrong permissions or content stop the node.
- A missing default file does not: the node starts, warns once, and rows of
  `VERIFIABLE` tables cannot be proven until it has one. For a node set up before
  ADR 0008, run `ledger-keygen -output` on its existing private key: it adds only the
  `.state` file and leaves the key pair as it is.
- Every node of a cluster needs the same state key, as it needs to trust the same
  public keys: replicas build the same state tree only with the same secret. Keep it
  as secret as the private key. Rotating the signing key does not change it; replacing
  the state key changes every salt, so it is never overwritten.

## Signed query results

With `ledger=signed`, a node can sign the result of a SELECT at a committed LSN
([ADR 0006](../adr/0006-signed-query-results.md)), so a third party can check later
which node gave it, against which history, and whether it is right:

```bash
# Ask the node: rows plus a signed attestation, as JSON.
asqlctl -command attest-query -pgwire 127.0.0.1:5433 \
  -sql "SELECT id, result FROM lab.results ORDER BY id" [-lsn 42] -output result.json

# Check it: signature, rows, chain (with a digest), re-execution (with the data).
asqlctl -command verify-result -result result.json -pubkey node.pub \
  [-digest digest.json] [-data-dir /copy/of/data]
```

`verify-result` reports each check apart and exits non-zero if one fails:

- **signature**: against the trusted keys in `-pubkey` only;
- **rows**: the rows in the file against the attested hash, row count and columns;
- **chain**: the attested chain value against a digest at the same LSN, or, with
  `-data-dir`, against the WAL, which must also match the digest when one is given;
- **reexecution**: with `-data-dir`, the query re-executed at the LSN on a scratch copy
  gives the attested rows. A different engine version that gives other rows is
  reported as `version-mismatch`.

Without `-data-dir`, a verifier relies on the node for the rows themselves: the
signature proves the node said so, and a digest proves which history it said it
about. `attest_query` needs the privileges of the query it attests, plus
`SELECT_HISTORY` at an LSN other than the head.

## Consistency proofs between digests

With `ledger=signed`, each digest also signs the root of the node's log tree, an
RFC 6962 Merkle tree over every WAL record up to the digest's LSN
([ADR 0008](../adr/0008-inclusion-and-consistency-proofs.md)). Whoever keeps an
earlier digest can then check that a later one extends the same history, without
seeing any record:

```bash
# Ask the node for a proof from the digest at LSN 120 to the one at LSN 980
# (without -to-lsn, to the latest digest).
asqlctl -command prove-consistency -pgwire 127.0.0.1:5433 \
  -from-lsn 120 [-to-lsn 980] -output proof.json

# Check it with the trusted keys alone: both signatures and the proof.
asqlctl -command verify-consistency -proof proof.json -pubkey node.pub
```

The same proof is `SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = 120
[AND to_lsn = 980]`, one row with `from_digest`, `to_digest` and `proof` as JSON; it
needs `ADMIN`, like `asql_admin.ledger_digest`. Proofs exist only between LSNs where a
digest was signed; asking for another LSN names the next signed one.

- The proof file carries both digests. Compare its `from_digest` with the copy you
  kept: a node that rewrote its history after it cannot produce a valid proof to it.
- `verify-ledger -digest` rebuilds the tree from a data directory and checks the
  digest's `tree_root` too.
- A node whose WAL does not start at LSN 1 (truncated before it had a tree) has no
  tree: it signs version 1 digests, without `tree_root`, and proofs are refused.

## Historical authorization rule

Historical authorization is evaluated against the **current** durable
principal/grant state even when the data snapshot being queried is older.

That means:

- the target data may come from an older `LSN` or logical timestamp,
- the authorization decision is made using the principal's current effective
  grants,
- ASQL does not invent a backdated principal history.

Practical consequence:

1. a principal can be created today,
2. `SELECT_HISTORY` can be granted today,
3. that principal may then inspect older snapshots,
4. the audit trail records that the historical read was authorized by the
   current grant state.

## Password rotation flow

Password rotation is a durable-principal operation.
Do not treat operator-token rotation as a replacement for user password
rotation.

Use the CLI alias path:

```bash
printf 'rotated-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token write-secret \
	-password-stdin \
	security user alter analyst
```

The same operation is also available through the Studio `Security` area and the
admin API password-set path.

## Worked security workflows

The examples below show the current intended operator flow:

1. bootstrap the first admin principal,
2. create a normal reader principal,
3. verify that current reads work but historical reads are denied,
4. grant explicit historical access,
5. verify that historical reads now work,
6. revoke historical access,
7. verify that the denied path returns.

For the examples below, assume the server was started with one shared operator
token for simplicity:

```bash
go run ./cmd/asqld \
	-addr :5433 \
	-admin-addr :9090 \
	-data-dir .asql \
	-auth-token admin-secret
```

### Example 1: bootstrap the first admin principal

```bash
printf 'admin-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	-password-stdin \
	-command principal-bootstrap-admin \
	-principal admin
```

After this step, steady-state principal management should happen through the
durable-principal model rather than by changing the operator token.

### Example 2: create a reader principal

```bash
printf 'reader-pass\n' | go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	-password-stdin \
	security user create reader
```

Inspect the resulting principal:

```bash
go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	security user show reader
```

At this point, `reader` is a normal authenticated principal without explicit
historical access.

### Example 3: verify the denied historical path before grant

Current-state reads should work:

```bash
PGPASSWORD='reader-pass' psql "postgres://reader@127.0.0.1:5433/asql?sslmode=disable" \
	-c "SELECT id, email FROM app.users ORDER BY id ASC LIMIT 1;"
```

Historical reads should still be denied because `reader` does not yet have
`SELECT_HISTORY`:

```bash
PGPASSWORD='reader-pass' psql "postgres://reader@127.0.0.1:5433/asql?sslmode=disable" \
	-c "SELECT * FROM app.users FOR HISTORY WHERE id = 1;"
```

That denied path is expected and is part of the current security model.

### Example 4: grant explicit historical access

```bash
go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	security grant history reader
```

Inspect effective historical access:

```bash
go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	security who-can history
```

After the grant, the same reader can run historical queries even if the target
rows were committed before the reader principal existed. The authorization
decision is based on the reader's **current** grant state.

### Example 5: verify the allowed historical path after grant

```bash
PGPASSWORD='reader-pass' psql "postgres://reader@127.0.0.1:5433/asql?sslmode=disable" \
	-c "SELECT * FROM app.users FOR HISTORY WHERE id = 1;"
```

You can also use `AS OF LSN` or `AS OF TIMESTAMP`; the same explicit
`SELECT_HISTORY` rule applies.

### Example 6: revoke historical access

```bash
go run ./cmd/asqlctl \
	-admin-http 127.0.0.1:9090 \
	-auth-token admin-secret \
	security revoke history reader
```

### Example 7: verify the denied path again after revoke

```bash
PGPASSWORD='reader-pass' psql "postgres://reader@127.0.0.1:5433/asql?sslmode=disable" \
	-c "SELECT * FROM app.users FOR HISTORY WHERE id = 1;"
```

After revoke, the historical read should be denied again while ordinary current
reads can still succeed.

## Management surfaces

Current supported management surfaces are:

- Studio `Security`
- `asqlctl`
- admin HTTP security endpoints

Current `asqlctl` ergonomic paths include:

- `security user create`
- `security user alter`
- `security user disable|enable|delete|list|show`
- `security role create|disable|enable|delete|list|show`
- `security grant history`
- `security revoke history`
- `security who-can history`

These guided paths are the current operator-facing security workflow.

## PostgreSQL role-management status

ASQL does **not** currently accept PostgreSQL role-management SQL as the public
durable-principal management path.

That means statements such as these should still be treated as unsupported for
principal lifecycle management over pgwire SQL:

- `CREATE USER ...`
- `CREATE ROLE ...`
- `ALTER USER ...`
- `ALTER ROLE ...`
- `DROP USER ...`
- `DROP ROLE ...`
- `GRANT role_name TO principal`
- `REVOKE role_name FROM principal`
- `GRANT SELECT_HISTORY ...`
- `REVOKE SELECT_HISTORY ...`

Use the explicit durable-principal management surfaces instead:

- Studio `Security`
- `asqlctl security ...`
- admin HTTP security endpoints

For the compatibility-oriented statement matrix and translation guide, see
[postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md).

## Audit expectations

ASQL records security-relevant events for the current durable-principal model,
including current login posture, denied historical reads, and recent
security-relevant changes.

When historical access is evaluated, the audit output is expected to make both
facts visible:

- the historical target that was requested,
- the current grant state under which the read was allowed or denied.

Studio's `Security` area also exposes a recent security activity view for
failed authz checks and recent security changes.

## Recommended operator posture

- use operator tokens to protect process/admin surfaces,
- bootstrap one admin principal early,
- treat durable principals as the long-term database access model,
- grant `SELECT_HISTORY` explicitly instead of bundling it implicitly with
  current reads,
- rotate user passwords through the durable-principal workflow,
- use Studio or `asqlctl security who-can history` to inspect effective
  historical access.

## Related docs

- [../getting-started/05-time-travel-and-history.md](../getting-started/05-time-travel-and-history.md)
- [../getting-started/08-studio-cli-and-daily-workflow.md](../getting-started/08-studio-cli-and-daily-workflow.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [postgres-compatibility-evidence-v1.md](postgres-compatibility-evidence-v1.md)