# Value realization plan: adopting `cdc-to-agent-events-lab`

> **Important notice**
>
> - **Fictional shipper.** Ruta Sur Logística is a company **invented** for this lab. Mailboxes use `@rutasur.example`. It does not represent any real client or employer.
> - **Inspired by the AWS Business Value Realization (BVR) framework:** a buyer, a problem, levers, KPIs with a baseline and a target, a cost view, and a measurement plan. **It is not an official AWS template**, and **I do not hold the AWS BVR Competency**, which is awarded to AWS partner companies, not to individuals.
> - Labels used below, and only these:
>   - **MEASURED**: a command in this repo was run, and the number is what it printed.
>   - **[EST]**: a figure this repo already marks as an estimate. This plan does not add a new one.
>   - **SIMULATED**: a seeded scenario or the simulated clock. Not a wall-clock service level.
>   - **UNVERIFIED**: named in the docs as not measured. No number is supplied here to fill the gap.
> - This plan does not invent a second customer, a yearly volume, an hourly rate, or an integration quote. [docs/COST.md](COST.md) already refuses a cluster bill of materials because no quote was collected. Contradicting that refusal would be a new number.
> - Package 0.1.0. The pytest row was measured on 2026-10-22 against commit `e82c512`, Python 3.12.3. The 2026-10-17 gate and bench were recorded against `f458fa4`. The package sources of those two commits match; only `README.md` differs.

---

## 1. Buyer and problem

**Buyer in the story.** Operations at the fictional shipper, who want a premium customer told when a shipment is delayed or when the promised date slips. **Reader of this plan.** An engineer or a buyer reviewing change-data capture and an agent trigger. That split is the audience line in [docs/BUSINESS_CASE.md](BUSINESS_CASE.md).

**Problem.** A delayed premium shipment should become one notice. The change event may be delivered twice, may arrive out of order, or the process may stop after the state write and before the offset commit. A poison record must not block other keys. A person approves the notice. The software does not send it.

**What has to be true before anyone prices a rollout.** [docs/BUSINESS_CASE.md](BUSINESS_CASE.md) already lists it: the source emits an ordered LSN per key; the effect (state write, outbox row) is idempotent; a named person approves notices and replays; the kill switch is tested before any send path exists. None of those is demonstrated on a live database here.

## 2. What the lab actually does

- An in-memory broker delivers at least once. The store applies each `event_id` once, in the same transaction as the dedupe row. The offset commits after that. A lower LSN does not overwrite a higher one. Poison goes to a dead-letter queue with the original bytes, and the offset still commits. [docs/DELIVERY_SEMANTICS.md](DELIVERY_SEMANTICS.md), ADR 0002, ADR 0004.
- A stub agent matches two rules: premium customer and status becoming `delayed`, or a promised date that slips by more than `CDC_DATE_SLIP_DAYS` (default 2). Notices stay `pending`. `send` raises `NotApproved` until a non-empty approver name is stored. `CDC_AGENT_ENABLED=0` keeps consuming and does not call the cassette. ADR 0003.
- The cassette is local text. Token counts are 0 by construction. There is no model client.
- The gate checks eight invariants over 10 scenarios. A forced regression turns idempotency off and must fail. [docs/METRICS.md](METRICS.md).

**MEASURED**, from commands named in the README:

- Eval: 0 violations, 10 scenarios, exit 0 (2026-10-17, and the same gate text on 2026-10-22).
- Regression: exit 1, event `evt-000001` applied 5 times (same dates).
- Pytest: 68 passed, 95.55% line coverage (2026-10-22, Python 3.12.3, pytest 9.1.1). The 2026-10-17 row is the same pair of numbers.
- Secret scan on 2026-10-22: `secret scan: no findings`.
- `agent_breaker`, re-run in-process on 2026-10-22: 4 notices, all pending. That is the count the business case prices.
- Bench: 2000 events, 17.276627 s, 115.76 events/s, 0 violations, 1 sample (2026-10-17 only). In-memory. Not a Kafka benchmark. Not re-run on 2026-10-22.

**SIMULATED** counters for the other scenarios are the table in [docs/METRICS.md](METRICS.md). They were reproduced on 2026-10-22 with the same integers. `python3 -m cdc_lab demo` is a **different** 80-event mix (applied 59, stale 16, duplicates 3, dead-letter 2, 10 pending notices on 2026-10-22). Do not add those 10 to the 4 priced notices.

## 3. Value levers

Each lever is something the code does. A dollar figure is attached only where [docs/BUSINESS_CASE.md](BUSINESS_CASE.md) already has one.

| Lever | What the lab shows | Money in this repo |
| --- | --- | --- |
| One effect per event | Duplicate burst: 5 deliveries, 1 apply. Regression with idempotency off: the same event applied 5 times, exit 1 | No separate price. The story prices notices, not duplicate writes |
| One notice per premium delay | `agent_breaker`: 4 pending notices, breaker opens and closes (3 failures) | 4 × 12 USD = 48 USD **[EST]**, and only if each notice is accurate |
| Poison does not stall other keys | `poison_middle`: 1 dead letter, 3 applies. YAML mix: 7 dead letters, the identity still closes | Not priced. No quote for a stalled partition |
| A person before any send | `send` raises `NotApproved`. The suite's unapproved-send count stays 0 | Not priced. There is no send path to audit |
| Kill switch | `CDC_AGENT_ENABLED=0` records suppressed and still consumes | Not priced |
| Replay of a state write | Fix profiles `identity` and `accept_schema_v2`. Replay of a sender is documented as unsafe | Not priced |
| Default-path software bill | No paid API, no broker started by CI | 0 USD for the lab run |

The robust value the code can defend is control of the state write: one apply, poison aside, notice still pending. Speed and a support-cost saving are the **[EST]** story, and they depend on a reject rate that is **UNVERIFIED**.

## 4. Baseline vs target KPIs

| KPI | Category | Baseline | Target | How it is measured here | Stage |
| --- | --- | --- | --- | --- | --- |
| Invariant violations on the 10 scenarios | Operational | Regression exits 1 when idempotency is off | 0 violations, exit 0 | `python3 -m cdc_lab eval` | Now, in CI |
| Applies of one duplicated event | Resilience | 5 applies of `evt-000001` with idempotency off (MEASURED) | 1 apply | `exactly_once_effect` | Now |
| Accounting identity | Resilience | Would fail `no_loss` if a record disappeared | applied + stale + duplicates + dead-letter = produced | Gate, non-vacuous on all 10 | Now |
| Unapproved sends | Risk | The invariant counts them | 0 | `agent_budget` | Now. No live sender exists to compare |
| Notices from `agent_breaker` | Experience | 4 shipments become delayed in that scenario (SIMULATED) | 4 notices, all `pending` | In-process re-run 2026-10-22 | Now |
| Lag at the end of `lag_drain` | Operations | Peak 20 records on the simulated clock | Final lag 0, alert recorded | SIMULATED. Threshold in that file is 5 records, not the process default of 1000 | Now, not a broker SLO |
| Reviewer reject rate | Quality | **UNVERIFIED** | Not set. The 48 USD **[EST]** assumes the rate is zero | [docs/BUSINESS_CASE.md](BUSINESS_CASE.md) says the measurement does not exist | Later, if a person reviews real notices |
| Support contacts avoided | Cost | **UNVERIFIED** | 1 contact per accurate notice **[EST]** | Assumption, not a count | Later |
| Kafka or Postgres lag and throughput | Capacity | **UNVERIFIED** | Do not publish the bench as either | Bench is one in-memory sample | Only with a separate log |
| Live model cost and latency | Cost | **UNVERIFIED**. "Measured with real providers: none yet" | Not set | No client in the package | Only with a separate log |

`retries_bounded` is non-vacuous in 1 scenario, `poison_isolation` in 4, `dlq_replay` in 2. A green row on the others is a real check; a green row on those three is a real check only in the scenarios that exercise them.

## 5. ROI model

**Formula:** ROI = (benefit − cost) / cost. Payback in months is not computed, because this repo has no initial-investment figure.

**Assumptions, every one already in [docs/BUSINESS_CASE.md](BUSINESS_CASE.md) or [docs/COST.md](COST.md):**

| Id | Assumption | Value | Label |
| --- | --- | --- | --- |
| A1 | Premium-delay notices in `agent_breaker` | 4 | SIMULATED scenario, reproduced 2026-10-22 |
| A2 | Support contacts avoided per accurate notice | 1 | **[EST]** assumption, not a measurement |
| A3 | Loaded cost of one support contact | 12 USD | **[EST]**. Not a vendor price and not a contract |
| A4 | Benefit of A1 × A2 × A3 | 48 USD | **[EST]** |
| A5 | Every one of the 4 notices is accurate | required for A4 | **UNVERIFIED**. Reject rate is not measured |
| A6 | Cost of the default lab path (eval, demo, tests) | 0 USD | No paid API. MEASURED as "no bill", not as a timed invoice |
| A7 | Kafka cluster, database, and on-call | not estimated | [docs/COST.md](COST.md). No quote was collected |
| A8 | A future live model | not measured | No price captured |

**Arithmetic that is defined:**

- Benefit in the story = 48 USD **[EST]**, one scenario, not a year, and only if A5 holds.
- Software bill for the measured path = 0 USD.
- Net of that story against that bill = 48 USD **[EST]** minus 0 USD.

**Arithmetic that is not defined:**

- (48 − 0) / 0 is not a percentage. A return ratio needs a non-zero cost. Inventing one would contradict A7.
- If a later quote sets a rollout cost C USD, the same formula is (48 − C) / C, and 48 is still only this scenario under A5. C is not in this repo. This plan does not pick a C.
- Payback months are not stated. There is no initial investment to divide by.

**Honest reading.** The base "saving" disappears if reviewers reject notices, because A5 is untested. It also disappears as a business result if the 12 USD is the wrong loaded cost; the business case already says to replace it with the operator's figure. The part that does not depend on A3 or A5 is the control: the gate fails when the same event is applied 5 times, and the breaker scenario leaves 4 notices pending.

## 6. Cost table

No vendor price list is stored in this repository. Figures below are copied from [docs/COST.md](COST.md), plus the 2026-10-22 re-run of the gate, the secret scan, and the demo, which also made no paid call.

| Scenario | Provider | Units | Cost / run | Latency | Quality | Label |
| --- | --- | --- | --- | --- | --- | --- |
| Eval gate, 10 scenarios | none, in-process | 0 | 0 USD, local CPU | not measured (simulated clock) | 0 violations, 2026-10-17 and again 2026-10-22 | MEASURED as no bill |
| Forced regression | none | 0 | 0 USD | not measured | exit 1, as required, both dates | MEASURED as no bill |
| Stub cassette | `stub-cassette`, local text | tokens in 0, tokens out 0 | 0 USD | not measured | notices stay `pending` | by construction |
| Demo, 2026-10-22 | none | 80 seeded events | 0 USD | not a percentile | 0 violations, 10 pending notices | SIMULATED counters, no bill |
| In-memory bench, 1 sample | none | 2000 events | 0 USD | elapsed 17.276627 s, not a percentile | 0 violations | MEASURED 2026-10-17. Not re-run 2026-10-22. Not a Kafka benchmark |
| Optional compose Postgres and Redpanda | not started | not measured | not measured | not measured | not run | **UNVERIFIED** |
| A future live model | not implemented | not measured | not measured | not measured | not measured | **UNVERIFIED**. No price captured |
| Laptop-sized process for this lab | n/a | n/a | 0 USD marginal | n/a | n/a | **[EST]** in COST.md |
| Kafka cluster, a database, and on-call | n/a | n/a | not estimated | n/a | n/a | out of scope. No quote was collected |

## 7. Measurement plan

What is already measured, and how to repeat it:

1. Unit and integration tests: `python3 -m pytest --cov=cdc_lab --cov-report=term-missing --cov-fail-under=80`. Record the date, the Python version, the pytest version, the passed count, and the `Total coverage` line. On 2026-10-22 that line was 95.55% (68 passed, Python 3.12.3, pytest 9.1.1). The coverage table's total column showed 96% for the same 2516 statements and 112 misses. Quote the `Total coverage` line.
2. Gate: `python3 -m cdc_lab eval`. Expect exit 0 and `violations: 0`.
3. Regression: `python3 -m cdc_lab eval --regression`. Expect exit 1 and a single `exactly_once_effect` violation for `evt-000001` applied 5 times. A zero exit means the gate is broken.
4. Secret scan: `python3 -m cdc_lab.secretscan`. Expect `secret scan: no findings`.
5. Demo: `python3 -m cdc_lab demo`. Check that applied + stale + duplicates + dead-letter equals produced, that the clock line says simulated, and that notices are pending. Do not paste those counters into the YAML `mixed_faults` row. They are a different mix.
6. Bench, only if you intend to publish a new sample: `python3 -m cdc_lab bench --events 2000 --seed 7`. Keep the JSON label. Do not replace the 2026-10-17 row with a number you did not run. Do not call it Kafka throughput.

What this plan does **not** measure, and what a later operator would have to add in a separate note ([docs/ADAPT_TO_A_REAL_CLIENT.md](ADAPT_TO_A_REAL_CLIENT.md)):

- Reviewer reject rate, before treating 48 USD **[EST]** as a saving.
- The operator's own cost per contact, replacing 12 USD.
- Broker throughput and replication lag, on a broker that is actually running.
- A live model, only behind a budget, a tested kill switch, and an approval record.
- A replay approver's name. The CLI does not store one today.

Do not mix the simulated table and a live table in one row.

## 8. Risks

Qualitative ratings are copied from [docs/RISKS.md](RISKS.md). This plan does not add probabilities.

| Risk | Likelihood in this lab | Impact if the same bug shipped | What this repo does | Residual |
| --- | --- | --- | --- | --- |
| Duplicate delivery applies the effect twice | Expected under at-least-once | Double notice or double write | Unique `(consumer, event_id)` in one transaction. Regression shows the 5 applies when that is off | Broken if the effect itself is not idempotent |
| Out-of-order LSN overwrites newer state | Injected by the generator | Wrong shipment status | Ignore a lower LSN, count stale | A gap in the LSN is not filled |
| Poison blocks a partition | Injected | Lag on every key that shares that partition | Permanent errors go to the dead-letter queue and the offset commits | A bug that classifies poison as transient can still stall |
| Retry storm | Injected transient faults | Downstream overload | Attempt cap, full jitter, seeded RNG | Cap is per record, not a global rate limit |
| Agent spends without a budget | Story only | Cost and noise | Budget window and circuit breaker. Failures are not invocations | Budget is in-process, not shared |
| Notice sent without approval | Guarded in code | Customer-facing mistake | `send` raises `NotApproved` | No send path exists to audit |
| Replay of a non-idempotent side effect | Documented | Duplicate customer action | Replay is specified as safe only for the state write | A future sender must not reuse this replay blindly |
| Secret in the tree | Low | Credential leak | Secret scan, and the note in SECURITY.md | The scanner is pattern-based |
| Simulated lag read as a service level | Easy to do | Bad capacity plan | Labels say simulated. The bench says it is not a Kafka result | The bench number can still be quoted out of context |
| 48 USD **[EST]** quoted as a measured saving | Easy to do | A saving that assumes a reject rate of zero | BUSINESS_CASE says the reject rate is not measured | This plan repeats that limit and does not compute a percentage ROI |

## 9. MEASURED vs not

**MEASURED**

- Pytest on 2026-10-22: 68 passed, 95.55% line coverage, Python 3.12.3, pytest 9.1.1, package 0.1.0. Same passed count and coverage on 2026-10-17.
- Eval gate: 0 violations, exit 0, on 2026-10-17 and again on 2026-10-22.
- Regression: exit 1, `evt-000001` applied 5 times, both dates.
- Secret scan on 2026-10-22: no findings.
- Bench on 2026-10-17 only: 2000 events, 17.276627 s, 115.76 events/s, 0 violations, one sample, in-memory.
- Demo stdout on 2026-10-22: the seeded mix printed produced 80, applied 59, stale 16, duplicates 3, dead-letter 2, violations 0, 10 pending notices, clock simulated.
- `agent_breaker` on 2026-10-22: 4 pending notices. The other scenario integers match [docs/METRICS.md](METRICS.md).

**SIMULATED, not a service level**

- Every lag, backoff, and processing-latency figure from the scenario clock.
- The per-scenario counters. Reproducing them on a later day does not turn them into a broker measurement.

**[EST]**

- 12 USD per support contact.
- 48 USD for four accurate notices.
- 0 USD marginal cost of a laptop-sized process, as COST.md marks that cell.

**UNVERIFIED / not measured**

- Reviewer reject rate.
- Contacts actually avoided.
- Kafka throughput, Postgres replication lag, a running Debezium connector.
- Live provider latency, token cost, and quality.
- A shared budget, a recorded replay approver, multi-region order, a schema registry.
- Any percentage ROI or payback period. The only software bill in the measured path is 0 USD, and the cluster quote was deliberately not invented.
