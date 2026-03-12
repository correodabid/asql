# ASQL Pricing & Licensing Model v1

Status: draft for Beta commercialization (Epic O).

## 1) Packaging and license policy

ASQL is packaged as:

1. **ASQL Community** (open source)
   - Intended for local development, evaluation, and small production workloads.
   - License: **Apache-2.0**.
   - Includes core single-node engine capabilities and deterministic WAL/replay baseline.

2. **ASQL Enterprise** (commercial subscription)
   - Intended for production teams requiring SLAs, advanced operations support, and enterprise controls.
   - Licensed under a commercial agreement (per environment/node tier).
   - Includes enterprise-only capabilities and support entitlements.

## 2) Commercial entitlement boundaries

### Community includes
- Embedded/single-node runtime.
- Deterministic WAL/replay and time-travel baseline.
- Basic tooling (`asqld`, `asqlctl`, `asqlstudio`) and public docs.
- Community support via public issue tracker (best effort).

### Enterprise adds
- Production support with response-time objectives.
- Security advisory early-access channel and coordinated patch guidance.
- Operational guidance for HA/replication rollouts.
- Enterprise governance artifacts and deployment assistance.

## 3) Pricing model (list pricing, annual)

### ASQL Enterprise Standard
- **USD 12,000 / year** per production environment.
- Includes 2 named support contacts.
- Support window: business hours (Mon–Fri).

### ASQL Enterprise Business
- **USD 30,000 / year** per production environment.
- Includes 6 named support contacts.
- Priority handling + architecture advisory sessions.
- Support window: extended business hours.

### ASQL Enterprise Critical
- **Custom pricing**.
- 24x7 support target, escalation management, and dedicated technical account coverage.
- For regulated or high-availability deployments.

Notes:
- Non-production environments are included at no additional charge up to the number of licensed production environments.
- Taxes and region-specific legal terms are excluded from list pricing.

## 4) Support tiers and target response objectives

Severity levels:
- **S1**: production outage / severe data-path impact.
- **S2**: major degradation with workaround limitations.
- **S3**: partial issue with available workaround.
- **S4**: how-to / minor defect.

Target first-response objectives:

| Tier | S1 | S2 | S3 | S4 |
|---|---:|---:|---:|---:|
| Standard | 8 business hours | 1 business day | 2 business days | 3 business days |
| Business | 4 business hours | 8 business hours | 1 business day | 2 business days |
| Critical | 1 hour (24x7) | 4 hours (24x7) | 1 business day | 2 business days |

## 5) Upgrade and compatibility promise

For active Enterprise subscriptions:
- Access to all minor/patch releases.
- Backward-compatibility guidance tied to `docs/release-upgrade-compat-checklist-v1.md`.
- Security fix advisories and recommended upgrade windows.

## 6) Commercial guardrails for claims

When selling ASQL:
- Performance claims must reference published benchmark methodology.
- Determinism claims must be tied to replay-equivalence test evidence.
- Compliance messaging must avoid claiming out-of-the-box certification; ASQL provides technical controls that support customer validation programs.

## 7) Next linked artifacts (Epic O)

- `docs/support-policy-v1.md`.
- `docs/security-disclosure-policy-v1.md`.
- `docs/benchmark-one-pager-v1.md`.
- `docs/architecture-one-pager-v1.md`.
