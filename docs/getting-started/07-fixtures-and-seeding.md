# 07. Fixtures and Seeding

Fixtures are the recommended way to create reproducible demo, test, and benchmark data in ASQL.

## Why fixtures matter in ASQL

Because ASQL is deterministic, reproducible setup is especially valuable for:

- demos,
- integration tests,
- benchmark baselines,
- debugging from known initial state.

## Fixture format

ASQL fixtures are strict JSON scenario files.
They contain ordered transaction steps with:

- a mode: `domain` or `cross`,
- participating domains,
- ordered SQL statements.

Reference:

- [../fixture-format-and-lifecycle-v1.md](../fixture-format-and-lifecycle-v1.md)

## Validate a fixture

```bash
go run ./cmd/asqlctl -command fixture-validate \
  -fixture-file fixtures/healthcare-billing-demo-v1.json
```

Validation includes:

- spec validation,
- non-determinism checks,
- dry-run execution on a fresh ephemeral engine.

## Load a fixture

```bash
go run ./cmd/asqlctl -command fixture-load \
  -pgwire 127.0.0.1:5433 \
  -fixture-file fixtures/healthcare-billing-demo-v1.json
```

## Export a fixture

```bash
go run ./cmd/asqlctl -command fixture-export \
  -pgwire 127.0.0.1:5433 \
  -domains billing,patients,clinical \
  -fixture-file fixtures/healthcare-billing-export-v1.json
```

## Studio workflow

Studio provides a dedicated `Fixtures` tab for:

- picking a fixture file,
- validating it,
- loading it,
- exporting selected domains into a deterministic fixture file.

## Best practices

- keep IDs and timestamps explicit,
- treat fixtures as scenario assets, not just table dumps,
- prefer small, meaningful business scenarios,
- validate fixtures before commit.

## Next step

Continue with [08-studio-cli-and-daily-workflow.md](08-studio-cli-and-daily-workflow.md).
