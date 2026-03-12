# ASQL Support Policy (v1)

Date: 2026-03-01
Applies to: ASQL Enterprise subscriptions and ASQL Community best-effort support.

## 1) Scope

This policy defines support channels, severity levels, response objectives, and customer responsibilities.

It complements:
- `docs/pricing-licensing-model-v1.md`
- `docs/incident-runbook-v1.md`

## 2) Support channels

### ASQL Community
- Channel: public GitHub issues/discussions.
- Coverage: best effort, no SLA commitment.
- Intended use: bugs, documentation gaps, feature requests.

### ASQL Enterprise
- Channel: private support ticket system + email escalation.
- Coverage: according to subscribed tier (Standard, Business, Critical).
- Intended use: production incidents, upgrade assistance, architecture guidance.

## 3) Severity classification

- **S1 / Critical**: production outage or severe data-path impact with no reasonable workaround.
- **S2 / High**: major functionality degraded, limited workaround.
- **S3 / Medium**: partial issue with acceptable workaround.
- **S4 / Low**: informational request, minor defect, or documentation question.

## 4) Target first-response objectives

| Tier | S1 | S2 | S3 | S4 |
|---|---:|---:|---:|---:|
| Standard | 8 business hours | 1 business day | 2 business days | 3 business days |
| Business | 4 business hours | 8 business hours | 1 business day | 2 business days |
| Critical | 1 hour (24x7) | 4 hours (24x7) | 1 business day | 2 business days |

Notes:
- Response objective is for first acknowledgement and triage start, not final resolution.
- Final resolution time depends on complexity, reproducibility, and dependency constraints.

## 5) Supported versions

For Enterprise support eligibility:
- Latest GA minor version is fully supported.
- Previous GA minor version is supported for security and critical fixes.
- Versions older than two minor releases are considered out of standard support scope.

## 6) Customer responsibilities

To receive effective support, customers should provide:
- version, deployment topology, and runtime flags;
- incident timestamp, impacted domains/workloads, and business impact;
- relevant logs and reproducible steps where possible;
- confirmation of backup status before high-risk remediation actions.

## 7) Exclusions

Support does not include:
- custom feature development under standard support;
- root-cause analysis for third-party systems outside ASQL control;
- issues from unsupported versions/environments without upgrade path.

## 8) Escalation and incident handling

- S1/S2 tickets are triaged immediately under subscribed response objectives.
- For Critical tier S1, incident bridge/escalation path is activated.
- Incident coordination follows the practices in `docs/incident-runbook-v1.md`.

## 9) Policy updates

This document is versioned. Material changes are announced in release notes and become effective on the stated revision date.
