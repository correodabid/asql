---
name: commit-and-pr
description: How changes land in this repo — commit conventions, pre-commit checks, when to open a PR vs push directly.
---

# commit-and-pr

This skill covers the last 50 meters: the change works, now get it in.

## Before staging anything

```bash
gofmt -w .
golangci-lint run
go test -timeout 300s ./...
```

CI runs all three. Catching them locally saves a round trip.

If you bumped any dep, also:

```bash
go mod tidy
govulncheck ./...
```

## Commit message format

```
<type>(<scope>): <imperative summary>

<why, not what — the diff shows what>

<optional footer, including Co-Authored-By>
```

- **type**: `feat`, `fix`, `chore`, `docs`, `refactor`, `style`, `test`
- **scope**: the subsystem touched. Seen recently: `pgwire`, `deps`,
  `ci`, `engine`, `wal`, `storage`. Omit when the change is truly
  cross-cutting.
- **subject**: under 72 chars, imperative ("add", "fix", not "added",
  "fixes"), no trailing period.
- **body**: explain *why* the change is correct and *why* it was
  needed. Describe side effects the reviewer might miss.

### Trailers

Always include when Claude contributed:

```
Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

### Examples from this repo

```
fix(pgwire): guard adminListener/adminServer with a mutex

chore(deps): bump pgx to v5.9.0 and adapt pgwire to new protocol semantics

chore(ci): bump Go to 1.25.9, grpc to 1.79.3, update lint action, drop wails deps

chore: remove example apps and non-technical docs
```

## Staging

Prefer explicit file lists over `git add .` — accidental staging of
`.asql/` data dirs or local binaries is the most common slip.

```bash
git add go.mod go.sum internal/server/pgwire/extended_query.go
```

## Safety rules

- **Never `git commit --amend` once pushed.** Create a new commit.
- **Never `git push --force`** unless the user explicitly asked. Never
  force-push main.
- **Never `--no-verify`.** If a hook fails, fix the underlying issue.
- **Never commit `.asql/`, `.asql-*/`, `.tmp/`, or build artifacts**
  (`asqld`, `asqlctl`, `seed_domains`, `*.test`). `.gitignore` handles
  them but `git add .` can still pull them in.

## When to open a PR vs push to main

This repo's `main` is the working branch. Small fixes, chores, and
docs can go directly to `main`. Open a PR when:

- The change touches cluster/replication/recovery and you want a second
  set of eyes.
- The change alters a wire-protocol contract or SQL surface.
- You're bumping a dep that affects multiple packages.
- The change is more than ~300 lines of non-mechanical code.

Otherwise, a descriptive commit on `main` is fine.

## After pushing

If CI goes red:

1. **gofmt**: `gofmt -w . && git add -u && git commit -m "style: gofmt"
   && git push`.
2. **Lint**: fix the specific complaint locally; don't blanket-disable
   rules.
3. **Test**: reproduce locally with the exact command from the CI log.
   Don't push a "try again" commit blind.
4. **govulncheck**: if a new CVE appears, bump the affected module and
   verify all packages still build and test.

## Reverting

```bash
git revert <sha>
```

Creates a new commit that undoes the change. Prefer this over
`git reset` once the commit is pushed.
