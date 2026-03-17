# ORM-lite adoption lane v1

This note defines the narrow, deliberately constrained application-integration
path ASQL currently recommends for teams evaluating existing PostgreSQL-oriented
service code.

Use it together with:

- [pgwire-driver-guidance-v1.md](pgwire-driver-guidance-v1.md)
- [postgres-compatibility-surface-v1.md](postgres-compatibility-surface-v1.md)
- [postgres-app-sql-translation-guide-v1.md](postgres-app-sql-translation-guide-v1.md)
- [../getting-started/09-go-sdk-and-integration.md](../getting-started/09-go-sdk-and-integration.md)

## Scope

This is not a claim of broad ORM compatibility.

It is a claim that a narrow ORM-lite or query-builder-lite service shape can
reach a first successful read/write flow when the application stays inside the
documented ASQL subset and translates the highest-friction PostgreSQL
assumptions explicitly.

Recommended use cases:

- small services with explicit SQL ownership,
- light query builders that let the team inspect emitted SQL,
- evaluation loops where the team wants to prove a real service workflow before
  deciding whether broader abstraction layers are worth validating.

## Recommended connection shape

Start with:

```text
postgres://asql@127.0.0.1:5433/asql?sslmode=disable&default_query_exec_mode=simple_protocol
```

Use `sslmode=prefer` instead of `disable` only when the client expects
PostgreSQL-style TLS negotiation and can fall back to plaintext.

Why this is the default:

- it keeps the SQL sent to ASQL easiest to inspect,
- it reduces surprises from driver-managed statement behavior,
- it makes it clearer whether a failure belongs to the app layer or the engine surface.

## Regression-covered happy path

The current lane is regression-covered in
[internal/server/pgwire/server_test.go](../../internal/server/pgwire/server_test.go#L635-L757)
via `TestPGWireORMLiteTranslatedHappyPath`.

That lane proves:

- connection over pgwire with `simple_protocol`,
- explicit `BEGIN DOMAIN ...`,
- `CREATE TABLE`,
- `INSERT ... RETURNING id`,
- parameterized `SELECT ... WHERE ... ORDER BY ... LIMIT ...`,
- `UPDATE` without `RETURNING`,
- `DELETE` without `RETURNING`,
- commit and post-commit verification,
- explicit guardrail guidance for `START TRANSACTION`.

## Translation rules for common PostgreSQL assumptions

| PostgreSQL-shaped assumption | Current ASQL path |
|---|---|
| `BEGIN` / `START TRANSACTION` | Use `BEGIN DOMAIN <name>` or `BEGIN CROSS DOMAIN <a>, <b>`. |
| `INSERT ... RETURNING ...` | Supported for the current documented insert-focused path. |
| `UPDATE ... RETURNING ...` / `DELETE ... RETURNING ...` | Treat as unsupported. Execute the mutation, then run a follow-up `SELECT` if the app needs the row shape back. |
| `ANY(ARRAY[...])` predicates | Translate to literal `IN (...)`, `IN (SELECT ...)`, or remodel the data shape. |
| implicit driver/ORM transaction ownership | Keep transaction scope explicit in application helpers so domain boundaries stay visible. |

## Safe subset checklist

Treat the lane as healthy when all of these are true:

- the team can inspect the emitted SQL,
- transactions are opened with explicit ASQL transaction primitives,
- writes use current supported `INSERT`, `UPDATE`, and `DELETE` shapes,
- any required `RETURNING` usage is limited to the documented `INSERT ... RETURNING` path,
- parameterized predicates stay within the currently documented scalar/bind subset,
- catalog-heavy tooling assumptions are validated separately rather than assumed from app success.

## Known unsupported edges for this lane

Do not treat the following as part of the ORM-lite claim:

- hidden bare PostgreSQL transaction syntax,
- `UPDATE ... RETURNING` / `DELETE ... RETURNING`,
- arrays and `ANY(...)`,
- full PostgreSQL catalog/type parity,
- full ORM metadata/discovery flows,
- generic proof that an arbitrary ORM works unchanged.

## Recommended rollout order

1. prove the service flow with literal SQL over pgwire,
2. keep transaction helpers explicit and ASQL-native,
3. validate one narrow abstraction layer only after the literal-SQL path is green,
4. document every translated assumption in the app repository.

If the literal-SQL lane fails, fix the SQL/model assumptions first.
If the literal-SQL lane passes but the abstraction layer fails, treat it as
client-surface validation work rather than evidence that the ASQL engine model
is wrong.

For the next high-return PostgreSQL-shaped SQL rewrites beyond this narrow lane,
use [postgres-app-sql-translation-guide-v1.md](postgres-app-sql-translation-guide-v1.md).