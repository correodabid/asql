# 04a. Domain Modeling Guide

This guide helps teams find the first useful domain boundaries when adopting ASQL.

The goal is not to split everything.
The goal is to make real ownership and invariant boundaries explicit.

## What a domain should represent

In ASQL, a domain should usually represent one or more of these:

- a boundary of ownership,
- a boundary of invariants,
- a boundary of replay reasoning,
- a boundary of transaction scope.

Good domains make it easier to answer:

- what changes together,
- what can be explained together,
- what should replay as one coherent boundary,
- and what should stay independent unless atomicity is truly required.

## Start with these questions

Before splitting domains, ask:

1. which data must usually commit together to preserve one invariant?
2. which tables are owned by the same part of the application?
3. which state transitions do developers need to explain together during incidents?
4. where would cross-domain work be rare and meaningful rather than constant?

If you cannot answer those questions yet, use fewer domains first.

## A practical rule

Start with the smallest number of domains that makes ownership and invariants visible.

That usually means:

- one domain for a narrow first adoption,
- two or three domains once real boundaries are understood,
- cross-domain transactions only when the invariant truly spans those boundaries.

## Signals that a split is probably good

- one team or service area clearly owns the tables,
- the boundary has its own lifecycle and debugging questions,
- the data is often read together but does not always need atomic writes together,
- historical inspection is easier if the boundary is explicit.

## Signals that a split is probably too early

- every important write immediately becomes `BEGIN CROSS DOMAIN ...`,
- the team cannot explain why the boundary exists beyond naming preference,
- the split mirrors screens or APIs rather than invariants,
- developers start treating domains as mandatory prefixes for every table rather than meaningful boundaries.

## How to avoid cross-domain sprawl

- prefer one domain until a second boundary is clearly justified,
- do not split just because concepts are different nouns,
- do not split just because one UI screen shows two concepts together,
- review every repeated cross-domain flow and ask whether the boundary is wrong,
- keep orchestration in the application unless atomicity is essential.

## Example application shapes

### 1. Banking-style application

Possible first domains:

- `identity`
- `ledger`
- `payments`
- `risk`

Why this split can work:

- customer identity and contacts are not the same invariant boundary as balances,
- ledger state has strong accounting-style mutation history,
- payments may need their own request lifecycle,
- risk review semantics often evolve independently of transfer storage.

What not to do too early:

- create separate domains for every transfer subtype,
- move approval meaning into ASQL instead of keeping it in app code,
- split reporting/projections into domains before core write boundaries are stable.

### 2. Healthcare-style application

Possible first domains:

- `patients`
- `clinical`
- `billing`

Why this split can work:

- patient identity is not the same thing as clinical workflow state,
- billing has its own history and reconciliation concerns,
- cross-domain transactions become meaningful instead of constant.

What not to do too early:

- create a separate domain for every document type,
- encode compliance interpretation directly into the engine,
- split every medical sub-specialty into its own transactional boundary.

### 3. Commerce-style application

Possible first domains:

- `catalog`
- `orders`
- `inventory`

Why this split can work:

- catalog change history is different from order lifecycle history,
- inventory reservation may require explicit cross-domain reasoning,
- the boundaries match common ownership lines and debugging questions.

What not to do too early:

- create separate domains for carts, promotions, discounts, and fulfillment on day one,
- make every checkout step one atomic cross-domain transaction,
- confuse read-model joins with write-boundary requirements.

## Recommended first rollout

1. start with one or two domains,
2. instrument the first real write workflows,
3. observe where cross-domain work is actually needed,
4. only then split further if the boundary is repeatedly justified.

## Responsibility boundary reminder

- **Engine-owned**: explicit transaction boundaries, deterministic history, replay-safe temporal inspection.
- **App-owned**: workflow semantics, approval logic, actor meaning, projections and downstream orchestration.
- **Recommended integration pattern**: use ASQL to make boundaries visible, but keep business meaning outside the engine.

## Next step

Return to [04-domains-and-transactions.md](04-domains-and-transactions.md) or continue with [10-adoption-playbook.md](10-adoption-playbook.md).