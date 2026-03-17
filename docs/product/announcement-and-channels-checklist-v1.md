# ASQL Announcement and Channels Checklist v1

Date: 2026-03-15
Status: launch-readiness checklist draft for `v1.0.0-rc1` through GA.

## Purpose

This checklist turns the launch narrative into an execution list for public release.

Use it to coordinate:
- website updates,
- docs visibility,
- technical launch post publication,
- demo/video preparation,
- GitHub release notes,
- community and direct-share channels.

Use it together with:
- [launch-narrative-v1.md](launch-narrative-v1.md)
- [technical-launch-post-draft-v1.md](technical-launch-post-draft-v1.md)
- [../operations/release-upgrade-compat-checklist-v1.md](../operations/release-upgrade-compat-checklist-v1.md)
- [../operations/release-evidence-bundle-v1.0.0-rc1-template.md](../operations/release-evidence-bundle-v1.0.0-rc1-template.md)

## Launch rule

Do not widen public claims during launch preparation.

Every public statement should stay aligned with:
- the GA compatibility contract,
- the current compatibility surface,
- the benchmark and release evidence,
- the canonical pgwire runtime path.

## Message checklist

Before publishing anything, verify that all public materials say consistently:
- ASQL is a deterministic SQL engine built in Go,
- domains are explicit boundaries,
- replay-safe history and temporal inspection are first-class,
- pgwire is the main application-facing path,
- ASQL supports a pragmatic PostgreSQL-compatible subset,
- ASQL is not a drop-in PostgreSQL replacement.

## Required launch assets

### 1. Website
- [x] home page reflects the final launch narrative
- [x] docs landing reflects the canonical pgwire path and compatibility stance
- [x] Studio page reflects temporal + cluster/operator workflows, not only UI polish
- [x] links to getting-started, compatibility docs, and release/production docs are visible

### 2. Docs portal
- [x] README is aligned with launch messaging
- [x] getting-started path is the primary onboarding route
- [x] examples are grouped by adoption moment
- [x] compatibility and release docs are visible from the docs site
- [x] benchmark and production docs are visible for evaluators

### 3. Technical post
- [x] final long-form technical launch post chosen from the draft
- [x] examples and claims reviewed against current product state
- [x] clear "what ASQL is / is not" section included
- [x] evaluation path included (`asqld` + pgwire + docs/examples)

### 4. Demo / video
- [ ] one short walkthrough script exists
- [ ] demo uses the canonical runtime path
- [ ] demo shows at least one temporal workflow
- [ ] demo shows at least one domain boundary or cross-domain flow
- [ ] demo avoids unsupported or parity-claiming tool behavior

### 5. Release notes
- [ ] release notes summarize supported path and key value pillars
- [ ] release notes link to compatibility docs
- [ ] release notes link to getting-started
- [ ] release notes link to benchmark and production/readiness docs
- [ ] release notes include any narrowed claims or known limits explicitly

## Channel checklist

### GitHub
- [ ] release description prepared
- [ ] release artifacts attached
- [ ] release notes link to docs portal
- [ ] release notes link to benchmark/compatibility docs

### Website / docs
- [ ] final site pages deployed
- [ ] no stale copy remains on site pages
- [ ] hero copy and docs intro match launch narrative

### Technical community channels
Examples:
- engineering blog,
- Hacker News submission text,
- Reddit / specialized forum post,
- relevant Go/database communities.

Checklist:
- [ ] short announcement variant prepared
- [ ] long technical post available
- [ ] claims checked for precision
- [ ] benchmark and compatibility links ready

### Direct-share / partner channels
Examples:
- design partners,
- pilot users,
- investor/advisor updates,
- warm technical intros.

Checklist:
- [ ] short evaluator summary prepared
- [ ] quickstart link ready
- [ ] benchmark one-pager linked
- [ ] compatibility stance stated explicitly

## Recommended launch packet

Minimum packet for any public announcement:
- website home
- docs landing
- getting-started guide
- compatibility policy/surface docs
- benchmark one-pager
- technical launch post
- GitHub release notes

## Final pre-publish review

Before pressing publish:
- [ ] release evidence bundle is green
- [ ] compatibility freeze review is complete
- [ ] no unsupported feature is presented as shipped
- [ ] no page implies full PostgreSQL parity
- [ ] canonical runtime path is the first path shown
- [ ] examples and screenshots reflect current product behavior

## Bottom line

The launch should feel clear, precise, and trustworthy.

The goal is not to sound bigger than the product.
The goal is to make the right users immediately understand why ASQL exists and how to evaluate it safely.