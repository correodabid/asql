# pgwire driver and query-mode guidance v1

This note translates ASQL's PostgreSQL-compatible subset into practical client guidance.

Use it together with:

- [sql-pgwire-compatibility-policy-v1.md](sql-pgwire-compatibility-policy-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)

## Short version

For ordinary application integration, the default path is:

- pgwire,
- `pgx` or `pgxpool`,
- `sslmode=disable` or `sslmode=prefer`,
- and `default_query_exec_mode=simple_protocol` when you want the lowest-surprise adoption path.

ASQL supports both simple query flow and the extended query pipeline for the documented subset.
The recommendation below is about adoption risk, not only protocol capability.

## Recommended path vs known-risk path

| Client shape | Recommendation | Why |
|---|---|---|
| Go service with explicit SQL | Recommended | `pgx` gives direct control over SQL, transaction scope, and query mode. |
| Go service with `pgxpool` | Recommended | Same as above, with pooled connections. |
| CLI/smoke tests/scripts | Recommended | Use `asqlctl shell` or `pgx` with explicit transaction primitives. |
| `pgx` simple protocol | Lowest-risk path | Keeps behavior closest to literal SQL text sent by the application. |
| `pgx` extended query protocol | Supported, but validate intentionally | Safe within the documented subset; still not the first path to debug onboarding surprises. |
| PostgreSQL GUI tools (`psql`, DBeaver, DataGrip, pgAdmin`) | Supported for documented workflows | Good for interactive work, but not a reason to assume full PostgreSQL parity. |
| ORMs or query builders that emit broad PostgreSQL syntax | Known-risk path | They often assume broader PostgreSQL semantics than ASQL promises. |
| Drivers or tools that require TLS-only startup (`sslmode=require`) | Unsupported today | ASQL currently responds to `SSLRequest` with `N`. |

## Recommended connection shapes

### Go / `pgx`

Recommended default:

```text
postgres://asql@127.0.0.1:5433/asql?sslmode=disable&default_query_exec_mode=simple_protocol
```

Good fallback for tool interoperability:

```text
postgres://asql@127.0.0.1:5433/asql?sslmode=prefer&default_query_exec_mode=simple_protocol
```

Use `sslmode=prefer` when the client expects PostgreSQL-style TLS negotiation but can fall back to plaintext.

## Why simple protocol is the default recommendation

Use simple protocol first when:

- onboarding a new team,
- validating a new application path,
- debugging SQL compatibility surprises,
- or narrowing whether a problem belongs to the engine surface or the client abstraction.

Why:

- fewer surprises from driver-side statement preparation behavior,
- easier correlation between application SQL and actual server input,
- cleaner reproduction of integration issues.

This is a guidance default, not a statement that extended protocol is broken.

## When extended protocol is reasonable

Extended protocol is reasonable when:

- your application already uses parameterized queries heavily,
- you stay within the documented ASQL subset,
- and you have intentionally validated the exact driver/query shapes you rely on.

That is the normal path after the team has already established one stable simple-protocol baseline.

## Known-risk patterns

Treat these as review points during adoption:

- assuming bare PostgreSQL transaction syntax like `BEGIN` is acceptable,
- relying on ORM-generated SQL that you do not inspect,
- assuming catalog or metadata parity beyond the documented surface,
- requiring TLS-only pgwire startup,
- assuming every PostgreSQL driver feature implies matching ASQL engine semantics.

## Practical rollout rule

Use this rollout order:

1. prove the workflow with literal SQL and explicit `BEGIN DOMAIN ...` / `BEGIN CROSS DOMAIN ...`,
2. prove it with `pgx` simple protocol,
3. only then validate pools, GUI tools, or more abstract query layers.

If step 2 fails, debug the SQL and transaction model first.
If step 2 passes but step 3 fails, the issue is usually client-surface compatibility rather than engine semantics.

## What to document in each app repository

Every ASQL-integrating app should document at least:

- the canonical connection string shape,
- whether `simple_protocol` is required or only recommended,
- which driver(s) are blessed,
- which GUI/CLI tools are known-good,
- which higher-level abstractions are intentionally unsupported or unvalidated.
