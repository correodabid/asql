# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public issues,
discussions, or pull requests.**

Instead, use one of the following private channels:

1. **Preferred**: [open a private security advisory on GitHub](https://github.com/correodabid/asql/security/advisories/new).
   This is the fastest path and keeps the report confidential until a
   fix is ready to disclose.
2. Email the maintainer at the address listed on the maintainer's
   GitHub profile, with `[asql-security]` in the subject line.

You should receive an acknowledgement within **72 hours**. If you do
not, please follow up — your first report may have been missed.

## What to include

Give us enough information to reproduce and assess the issue:

- The version or commit SHA of ASQL you were running.
- Whether you were running `asqld` single-node, in cluster mode,
  or embedded via the Go SDK.
- Steps to reproduce, including any SQL, fixture, or pgwire client
  flow required.
- The impact: what can an attacker read, write, escalate to, or
  crash?
- Any proof-of-concept code or WAL / configuration samples.

## What you should expect

- **Acknowledgement** of the report within 72 hours.
- **An initial assessment** within 7 days, including whether we agree
  on the severity.
- **A patched release** within 30 days for confirmed high- or
  critical-severity issues. Longer timelines for lower-severity issues
  will be communicated up front.
- **Coordinated disclosure**: we will work with you on a public
  advisory timeline once the fix is available.

## Scope

In scope:

- Any component in the main `github.com/correodabid/asql` repository,
  including `cmd/asqld`, `cmd/asqlctl`, `cmd/asql-mcp`, and the
  packages under `internal/` and `pkg/`.
- The pgwire surface and the gRPC admin surface.
- The WAL / snapshot / recovery path.
- Supply-chain concerns (dependency vulnerabilities reachable from
  our code paths).

Out of scope:

- Issues in third-party dependencies that are not reachable from ASQL
  code paths — report those upstream.
- Theoretical vulnerabilities without a demonstrable impact on ASQL.
- Vulnerabilities requiring physical access to the server running
  `asqld`.
- Denial-of-service scenarios that require administrator-level access
  (they are features of the admin surface, not bugs).

## Supported versions

ASQL does not yet have a formal release cadence. Security fixes are
applied to the `main` branch. When tagged releases exist, security
patches will be backported to the latest minor version.

## Operational guidance

For the full operational security posture — secrets handling, audit
log review, incident response, disclosure-to-fix timelines — see
[docs/operations/security-disclosure-policy-v1.md](docs/operations/security-disclosure-policy-v1.md)
and the [Incident Runbook](docs/operations/incident-runbook-v1.md).

## Vulnerability scanning

We run `govulncheck ./...` in CI on every push to `main`. Contributors
are expected to run it locally after any dependency bump — see
`.claude/skills/govulncheck.md` or
[AGENTS.md](AGENTS.md#common-commands) for the workflow.

## Attribution

We are happy to credit reporters in the published advisory unless you
prefer to remain anonymous. Please tell us which you prefer when you
report.
