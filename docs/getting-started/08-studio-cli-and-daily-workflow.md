# 08. Studio, CLI, and Daily Workflow

A practical ASQL adoption usually uses both Studio and `asqlctl`.

## Use Studio for exploration

Studio is best for:

- browsing schema,
- running ad hoc queries,
- inspecting row detail,
- viewing mutation history and entity history,
- inspecting temporal helper values,
- exploring snapshots and diffs in Time Explorer,
- fixture validate/load/export workflows.

Start it with:

```bash
go run ./cmd/asqlstudio -pgwire-endpoint 127.0.0.1:9042 -data-dir .asql
```

## Use `asqlctl` for scripts and repeatable commands

`asqlctl` is best for:

- shell automation,
- CI or smoke tests,
- scripted transactions,
- fixture workflows,
- reproducible team instructions.

## Suggested daily loop

1. run `asqld` locally,
2. use Studio for interactive inspection,
3. use `asqlctl` for scripted flows,
4. create fixtures for realistic scenarios,
5. verify temporal behavior with Time Explorer and helper queries.

## Recommended team habits

- keep a small stable fixture pack in the repository,
- document domain boundaries early,
- use Time Explorer for debugging instead of guessing from current state,
- prefer explicit transaction flows in examples and scripts.

## Next step

Continue with [09-go-sdk-and-integration.md](09-go-sdk-and-integration.md).
