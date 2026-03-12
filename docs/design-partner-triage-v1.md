# ASQL Design-Partner Feedback Triage Workflow (v1)

Date: 2026-02-28

## Purpose

Create a repeatable weekly workflow for collecting, prioritizing, and acting on pilot feedback.

## Cadence

- Intake: continuous
- Triage meeting: weekly (45 min)
- Decision publish: within 24h after triage
- Progress review: weekly

## Roles

- Product owner: final prioritization decision
- Tech lead: feasibility/risk assessment
- On-call engineer: operational impact classification
- Partner manager: partner communication owner

## Intake template (required fields)

Each feedback item must include:

1. Partner name and environment
2. Problem statement (what failed / what blocked)
3. Business impact (severity and urgency)
4. Reproduction steps or evidence
5. Desired outcome
6. Deadline/commitment context

## Classification model

Priority buckets:

- P0: production blocker / data risk
- P1: severe degradation with workaround
- P2: moderate friction
- P3: enhancement request

Type tags:

- reliability
- determinism
- performance
- security
- DX/onboarding
- docs

## Triage decision rules

1. If P0/P1 with reproducible evidence -> enter current sprint hotfix lane.
2. If issue affects >1 partner -> escalate one priority level.
3. If feature request has no clear pilot ROI within 8 weeks -> defer to backlog parking lot.
4. Always attach owner and target sprint before closing triage.

## Output artifacts per meeting

- Ranked triage board (Top 10)
- Decision log with rationale
- Owner + ETA per accepted item
- Partner-facing update summary

## KPIs

- Median triage lead time (intake -> decision)
- % feedback items with owner and ETA
- % P0/P1 resolved within SLA
- Partner satisfaction score (weekly pulse)

## Definition of done for a triaged item

An item is considered triaged only if:

1. Priority assigned
2. Type tag assigned
3. Owner assigned
4. Target sprint assigned
5. Partner communication sent