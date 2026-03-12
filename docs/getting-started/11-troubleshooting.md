# 11. Troubleshooting

## `connection refused`

Check:

- `asqld` is running,
- the endpoint is correct,
- Studio points to the same pgwire endpoint.

## `table not found`

Usually one of these is true:

- schema was never created,
- you are in the wrong domain,
- the query assumes implicit context that has not been established.

## history output looks different than expected

Use the stable metadata names:

- `__operation`
- `__commit_lsn`

Do not rely on older internal field names.

## versioned reference write fails

Check:

- the referenced row is visible,
- the referenced table is the entity root when using entity semantics,
- the reference is not pointing to a missing historical token.

## fixture validation fails

Typical reasons:

- missing dependency order,
- references to data not yet inserted,
- unsupported non-deterministic tokens such as `NOW()` or `RANDOM()`,
- transaction control statements embedded inside fixture steps.

## fixture export fails

The export path is intentionally strict.
Common causes:

- exported table has no primary key,
- selected domains omit dependency domains,
- dependency cycle prevents deterministic export.

## historical query confusion

Recommended sequence:

1. inspect `current_lsn()`,
2. inspect `row_lsn(...)` or `entity_version(...)`,
3. inspect `FOR HISTORY`,
4. run the exact `AS OF LSN` query.

## where to look next

- [README.md](../../README.md)
- [../getting-started-10-min.md](../getting-started-10-min.md)
- [../cookbook-go-sdk.md](../cookbook-go-sdk.md)
- [../fixture-format-and-lifecycle-v1.md](../fixture-format-and-lifecycle-v1.md)
- [../temporal-introspection-surface-v1.md](../temporal-introspection-surface-v1.md)
