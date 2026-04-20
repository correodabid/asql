---
name: govulncheck
description: Run govulncheck, interpret its output, and resolve Go CVEs in this repo (bump dep, rebuild, verify).
---

# govulncheck

Use this whenever you bump a Go dependency, whenever GitHub Dependabot
opens an alert, or before a release.

## The command

```bash
govulncheck ./...
```

If `govulncheck` is not installed:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
```

Clean output ends with `No vulnerabilities found.` Anything else means
at least one CVE reaches our code path.

## Interpreting the output

A finding looks like:

```
Vulnerability #N: GO-YYYY-NNNN
    <summary>
  More info: https://pkg.go.dev/vuln/GO-YYYY-NNNN
  Module: github.com/foo/bar
    Found in: github.com/foo/bar@v1.2.3
    Fixed in: github.com/foo/bar@v1.2.4
    Example traces found:
      #1: <our caller path>
```

Two cases:

1. **Example traces present** — our code actually reaches the vulnerable
   function. This is real. Fix it.
2. **No example traces** — the module is in the dep graph but we don't
   call the vulnerable function. Still worth bumping, but lower
   urgency.

## Fix procedure

### 1. Bump the module

```bash
go get github.com/foo/bar@v1.2.4
go mod tidy
```

Pick the **minimum** version marked `Fixed in`. Don't jump to a major
version "while we're at it" — that expands blast radius.

### 2. Check for API breakage

```bash
go build ./...
```

If it fails, the bump introduced breaking changes. Read the module's
`CHANGELOG.md` or release notes. Common shapes:

- **Type change** (e.g. `SecretKey: uint32 → []byte` in pgx v5.9):
  write a helper at the boundary to convert, don't propagate the new
  type through the codebase.
- **Renamed function**: rename at all call sites.
- **Removed function**: find the replacement in the module's docs.

### 3. Run the tests that exercise the changed path

If pgx changed, run pgwire tests:

```bash
go test -timeout 300s ./internal/server/pgwire/...
```

If the failing tests are **hangs** (not errors), suspect a protocol
change. See `.claude/skills/pgwire-feature.md` for the v5.9
`Describe Portal` trap.

### 4. Run the full suite

```bash
go test -timeout 300s ./...
```

### 5. Re-run govulncheck

```bash
govulncheck ./...
```

Expected: `No vulnerabilities found.`

### 6. Commit

```
chore(deps): bump <module> to <version> — fixes GO-YYYY-NNNN[, GO-YYYY-MMMM]
```

Body should summarize:

- which CVEs this resolves,
- any code changes required to adapt to the new version,
- whether tests caught real behavioral differences (not just API
  rename) — document those so the next person understands the risk.

Include `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>` when
Claude drove the work.

## When fixing is not trivial

If the fix requires a major version bump or significant adaptation:

1. Do the work on a branch, open a PR.
2. In the PR description, paste the full `govulncheck` output before
   and after.
3. Highlight any behavior changes the tests revealed (example: the
   pgx v5.9 bump changed `ExecStatement` to skip `Describe Portal`,
   which deadlocked streaming FOLLOW queries — not API-visible but
   wire-visible).

## CI posture

CI runs `govulncheck` on every push. Previously it was
`continue-on-error: true` while CVEs were open; once a bump lands it's
safe to flip that back to hard-fail.

## Don't

- Don't add modules to a govulncheck allowlist unless you have a
  written exception. The signal gets drowned out fast.
- Don't bump to a pre-release (`-rc`, `-beta`) as a CVE fix unless the
  module has no stable fix. Prefer waiting or vendoring a patch.
- Don't pin transitive deps in `replace` directives without tracking
  why — it's the kind of drift that causes the next person a bad day.
