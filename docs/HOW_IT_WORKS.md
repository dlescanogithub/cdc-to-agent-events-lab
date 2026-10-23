# How it is put together

This is a walkthrough for engineers and buyers. The company, Ruta Sur Logistics, is fictional. The "model" is a cassette. There is no real database behind it. The business figures in dollars are in the README, and they are [EST] or zero. This page does not invent others.

## The analogy

Picture the warehouse notebook. Each time a shipment changes, someone writes a line with a sequence number (the LSN). The consumer reads those lines. If the same line arrives twice, it does not write the state again. If a line arrives that is older than the one already applied, it counts that line as stale and does not overwrite the newer state. If the line is broken, it sets the line aside on a tray (the DLQ) and continues with the other keys. Only after that does a stub clerk check whether the customer is premium and the shipment became delayed, or whether the promised date slipped by more than N days. It leaves a draft. It does not send the draft. A person has to approve it.

The lab clock is a counter. It advances when the code says so. It is not wall-clock time.

## Path through the code

1. `python -m cdc_lab generate` builds synthetic events: creates, updates, deletes, and snapshot reads, plus faults on purpose (duplicates, out-of-order delivery, broken JSON, a schema fault, an unknown version, a large payload).
2. The in-memory broker assigns each key with SHA-256 (it does not use Python's hash, which changes from process to process). A commit advances the offset by at most one, and it cannot move backwards. If there is no ack, the same offset shows up again when the visibility lease expires.
3. The consumer decodes. If the message is poison, it goes to the DLQ with the original bytes, and the offset is committed so the other keys are not stuck.
4. If the downstream fails in a transient way, it retries with full jitter and a cap. The jitter comes from a seeded RNG. If the attempts run out, the record goes to the DLQ.
5. The effect and the dedupe mark are written in the same SQLite transaction (or in the in-memory store). The offset is committed after that.
6. If the process dies before the store commit (`before_store_commit`), the transaction rolls back and the retry applies once. If it dies after the store and before the offset (`after_store_before_offset`), on restart the effect is already recorded and is not applied again. The scenario `crash_after_effect` does that once: 5 events, and each effect count stays at 1.
7. An LSN older than the one already applied is counted as `skipped_stale` and does not overwrite the state.
8. Only then does the stub agent look at the rules. A premium customer whose shipment becomes delayed, or a promised date that slips by **more** than N days (the default of `CDC_DATE_SLIP_DAYS` is 2; the comparison is strict, not "2 or more"). It leaves a draft `pending`. It sends nothing. `send` raises `NotApproved` if there is no approver name. With `CDC_AGENT_ENABLED=0` it keeps consuming, marks the outbox row as `suppressed`, and does not call the cassette.
9. Several notices for the same shipment inside the coalesce window (default 5000 ms) are folded together. The window is per shipment key, not across the whole batch. The breaker opens after K consecutive failures, leaves the rows in the outbox, and can close after the cooldown.

The two cassette sentences are in `src/cdc_lab/cassette.py`. They say the notice is pending approval and that it was not sent. The token counts are 0 because the text is already in the repo. If the key is not in the cassette, that is an error. There is no network.

## Two different "mixed" runs

Both are named `mixed_faults` and both use seed 7 and 80 events. They are not the same mix.

- The file `scenarios/mixed_faults.yaml` asks for 8 duplicates and 7 poison records. The README table (and `docs/METRICS.md`) is applied 32, stale 33, duplicates 8, DLQ 7. 32 + 33 + 8 + 7 = 80. The stale count of 33 is not the 4 swaps in the file: they are different counters.
- `python -m cdc_lab demo` builds another mix in the CLI (3 duplicates, 1 malformed record, 1 schema violation). On 2026-10-22 it printed applied 59, stale 16, duplicates 3, DLQ 2, violations 0, `drafts_pending` 10, and `clock: simulated`. 59 + 16 + 3 + 2 = 80. Those 10 drafts are not the 4 in the business case.

The business case uses `agent_breaker`: 4 drafts, all `pending`. That was run again on 2026-10-22 and again returned 4. The price of 48 USD is [EST] (4 × 12) and is explained in `docs/BUSINESS_VALUE.md`. It is not a measurement from a contact center.

## What was measured

The detail and the date of each command are in the README, section Proof, and in `docs/METRICS.md`. In short, on 2026-10-22, Python 3.12.3, package 0.1.0:

- `python3 -m pytest --cov=cdc_lab --cov-report=term-missing --cov-fail-under=80`: 68 passed, line coverage 95.55% (the `Total coverage` line that pytest-cov prints). pytest 9.1.1.
- `python3 -m cdc_lab eval`: 0 violations, 10 scenarios, exit 0. The same text was already recorded on 2026-10-17.
- `python3 -m cdc_lab eval --regression`: exit 1. With idempotency off, `evt-000001` is applied 5 times.
- `python3 -m cdc_lab.secretscan`: no findings.

The bench of 2000 events (17.276627 s, 115.76 events/s, one sample, in memory) is from 2026-10-17. It was not run again on 2026-10-22. It is not a Kafka benchmark.

The eight invariants, in one sentence: no event is lost, each effect is applied once, the highest LSN wins, retries are capped, poison does not stall the other keys, lag ends at 0 or an alert is raised, the agent stays inside the budget and does not send without approval, and replay applies once whatever can be repaired. Retries, poison, and replay are not exercised in all 10 scenarios: the gate says so with `non_vacuous` (1, 4, and 2).

## Glossary

| Word | What it means here |
| --- | --- |
| LSN | Change sequence number. The highest one per key wins. |
| Offset | Position in the partition. Distinct from `event_id`. |
| DLQ | Queue of messages that cannot be applied. It keeps the original payload. |
| Effectively once | At-least-once delivery, plus an idempotent effect. It is not a Kafka transaction. |
| Cassette | A fixed response. If the key is missing, that is an error. There is no network. |
| Simulated clock | A counter that advances when the lab says so. It is not a wall clock. |
| Draft `pending` | Text the agent left behind. It has not gone out. `approve` needs a name, or `send` fails. |
| Kill switch | `CDC_AGENT_ENABLED=0`. Consumption continues. The cassette is not called. |

## Limits

- Debezium is not running, and neither a real Postgres nor Kafka is in the measurement. The compose file and the Dockerfile exist, and CI does not start them.
- The agent does not call a model. The cassette token counts are 0.
- Histogram latency is simulated time. The peak of 20 records in `lag_drain` is on that clock, with a threshold of 5 in that file, not the default of 1000.
- `bench` is a single in-memory sample, with JSON logs on stderr. It is not a Kafka benchmark.
- If the downstream sends mail or charges a card, replay is no longer safe. Here the effect is a state write.
- The replay CLI does not record the approver's name. `approve` on a draft accepts any non-empty name. It does not require a second person.
- Measured with real providers: none yet.
- A percentage ROI is not calculated. The measured cost of the default path is 0 USD, and there is no cluster quote. That is in `docs/BUSINESS_VALUE.md`.

## Short walkthrough

1. Run `python -m cdc_lab demo` and check that applied + stale + duplicates + dlq equals produced. That mix is not the YAML file.
2. Run `python -m cdc_lab eval` and read `violations: 0`.
3. Run `python -m cdc_lab eval --regression` and check that it exits with code 1: without idempotency the same event is applied five times.
4. The drafts stay `pending`, the kill switch keeps consuming but does not call the cassette, and the bench is not Kafka.
