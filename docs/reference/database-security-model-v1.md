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

This is not full PostgreSQL role/privilege parity.
It is the current ASQL-documented security surface.

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