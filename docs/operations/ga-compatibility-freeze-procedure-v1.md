# ASQL GA Compatibility Freeze Procedure v1

Date: 2026-03-15
Applies to: `v1.0.0-rc1` and later release candidates until GA ships.

## Purpose

This procedure turns the GA compatibility contract into an operational review.

Use it when preparing any release candidate or GA cut that wants to claim:
- stable pgwire / SQL behavior inside the documented ASQL subset,
- stable WAL / replay behavior for the supported upgrade window,
- stable backup / restore and operator workflows,
- and release evidence strong enough to support a GA freeze.

Use it together with:
- [../reference/asql-ga-compatibility-contract-v1.md](../reference/asql-ga-compatibility-contract-v1.md)
- [release-upgrade-compat-checklist-v1.md](release-upgrade-compat-checklist-v1.md)
- [release-evidence-bundle-v1.md](release-evidence-bundle-v1.md)
- [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md)

## Freeze rule

A behavior is frozen for GA only if it is:
1. documented,
2. regression-backed,
3. included in the release evidence bundle,
4. not marked experimental, partial, internal-only, or legacy-only.

If any of those are missing, the behavior is **not** yet frozen and must not be presented as a GA promise.

## Review cadence

Run this review:
- before each `v1.0.0-rc*` cut,
- before GA,
- and after any change window that touches pgwire startup/auth, SQLSTATE mapping, WAL interpretation, replay/recovery, backup/restore, or cluster failover behavior.

## Review workflow

### Step 1 — classify the intended contract surface

Review the candidate release and classify changed behavior into these buckets:
- SQL / pgwire application surface,
- WAL / replay,
- backup / restore / upgrade,
- operator surface,
- cluster continuity / failover,
- performance guardrails.

For each bucket, decide one of:
- `unchanged contract`
- `additive contract expansion`
- `internal-only change`
- `contract risk / narrowing`

If a change would narrow an already documented supported flow, stop and resolve it before release.

### Step 2 — review public docs first

Review the active truth surfaces:
- [../../README.md](../../README.md)
- [../getting-started/README.md](../getting-started/README.md)
- [../reference/sql-pgwire-compatibility-policy-v1.md](../reference/sql-pgwire-compatibility-policy-v1.md)
- [../reference/postgres-compatibility-surface-v1.md](../reference/postgres-compatibility-surface-v1.md)
- [../reference/pgwire-error-sqlstate-behavior-v1.md](../reference/pgwire-error-sqlstate-behavior-v1.md)
- relevant operator docs in [runbook.md](runbook.md), [incident-runbook-v1.md](incident-runbook-v1.md), and [telemetry-dashboard-v1.md](telemetry-dashboard-v1.md)

Questions to answer:
- do the docs still describe the canonical pgwire path first?
- do the docs still describe ASQL as a PostgreSQL-compatible subset rather than full parity?
- does any doc claim behavior that is no longer test-backed?
- did any real behavior become supported without the docs saying so?

If docs and behavior disagree, fix docs or code in the same release window.

### Step 3 — run the required evidence lanes

At minimum, gather evidence for:

#### Compatibility
- required lanes from [pgwire-compatibility-test-pack-v1.md](pgwire-compatibility-test-pack-v1.md)
- focused SQLSTATE/error-shape regressions when relevant

#### Determinism and recovery
- replay / restart parity
- backup / restore parity
- restore-boundary and corruption-detection tests

#### WAL compatibility
- version mismatch handling
- legacy fixture read where applicable
- append continuity / replay continuity checks

#### Operations
- runtime boot smoke
- admin HTTP health/readiness/metrics sanity
- leadership/failover/admin inspection endpoints

#### Cluster continuity
When cluster/runtime, promotion, fencing, or replicated commit behavior changed:
- failover continuity evidence
- replay state-hash continuity evidence

#### Performance guardrails
Treat these as required release evidence, not optional notes:
- single-node write scaling guardrail
- cluster append-growth guardrail

## Review output format

Record the review in the release evidence bundle with a dedicated GA contract section.

For the first RC, the recommended starting point is:
- [release-evidence-bundle-v1.0.0-rc1-template.md](release-evidence-bundle-v1.0.0-rc1-template.md)

Recommended fields:
- contract version reviewed
- reviewer/date
- release/tag under evaluation
- buckets touched
- contract changes: `none` / `additive` / `blocked`
- docs reviewed
- tests/lanes executed
- unresolved risks
- release decision

## Decision rules

### Release can proceed when
- no documented supported flow was silently narrowed,
- evidence lanes for affected areas are green,
- docs and behavior are aligned,
- compatibility claims remain explicit and bounded,
- performance guardrails are within threshold,
- rollback/upgrade story is still clear.

### Release must stop when
- a documented supported flow regressed,
- WAL/replay behavior changed without explicit version/migration handling,
- backup/restore evidence is missing or red,
- docs overclaim current compatibility,
- release evidence cannot show green or intentionally narrowed behavior.

## Handling contract changes

### Additive change
Allowed when:
- docs are updated,
- tests exist,
- evidence is included,
- the change does not narrow existing support.

### Narrowing or breaking change
Do not ship silently.
Required actions:
- update the contract or surface docs explicitly,
- add release-note and upgrade-note language,
- document migration/rollback guidance,
- treat the release as blocked until the narrowing is acknowledged and intentional.

## Suggested compact review template

```text
GA contract review
- contract: asql-ga-compatibility-contract-v1.md
- release/tag: <tag>
- date: <yyyy-mm-dd>
- reviewer: <name>

Buckets touched:
- sql/pgwire: unchanged|additive|risk
- wal/replay: unchanged|additive|risk
- backup/restore/upgrade: unchanged|additive|risk
- operator surface: unchanged|additive|risk
- cluster continuity: unchanged|additive|risk
- performance guardrails: green|blocked

Docs reviewed:
- README.md
- docs/getting-started/...
- compatibility policy/surface/evidence docs
- operations docs

Evidence executed:
- compatibility lanes: <list>
- replay/recovery lanes: <list>
- operator lanes: <list>
- guardrails: <list>

Decision:
- proceed|blocked

Notes:
- <summary>
```

## Bottom line

The freeze is real only when ASQL can prove that its documented subset, replay story, upgrade story, operator story, and performance guardrails all still hold in the same release window.

If it cannot prove that, the right action is to narrow claims or keep hardening, not to ship optimism.