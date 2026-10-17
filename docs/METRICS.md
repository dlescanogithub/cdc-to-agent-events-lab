# Metrics

## Measured

| Metric | Value | When | Command |
| --- | --- | --- | --- |
| Pytest | 68 passed, coverage 95.55% | 2026-10-17, Python 3.12.3, package 0.1.0 | `python3 -m pytest` |
| Eval gate | 10 scenarios, 0 violations | 2026-10-17 | `python3 -m cdc_lab eval` |
| Forced regression | exit 1, exactly-once fails (`evt-000001` applied 5 times) | 2026-10-17 | `python3 -m cdc_lab eval --regression` |
| In-memory bench, one sample | 2000 events, 17.276627 s, 115.76 events/s, 0 violations | 2026-10-17, Linux 6.12.94+, Python 3.12.3 | `python3 -m cdc_lab bench --events 2000 --seed 7` |

The bench label in the JSON is `measured, in-memory, not a Kafka benchmark`. Default INFO logging writes a JSON line per effect to stderr inside the timed section. It is one sample. It is not a confidence interval.

## Simulated by construction

Counters below come from the seeded scenarios on a simulated clock. They are not wall-clock SLOs.

| Scenario | Produced | Applied | Stale | Duplicates | DLQ | Note |
| --- | --- | --- | --- | --- | --- | --- |
| duplicate_burst | 5 | 1 | 0 | 4 | 0 | one effect |
| out_of_order | 2 | 1 | 1 | 0 | 0 | highest LSN kept |
| poison_middle | 4 | 3 | 0 | 0 | 1 | other keys proceed |
| transient_recovery | 1 | 1 | 0 | 0 | 0 | attempts recorded |
| permanent_replay | 2 | 2 | 0 | 0 | 0 | after identity replay |
| crash_after_effect | 5 | 5 | 0 | 0 | 0 | apply_count 1 |
| lag_drain | 20 | 20 | 0 | 0 | 0 | alert fired, lag ended at 0 |
| agent_breaker | 5 | 5 | 0 | 0 | 0 | breaker opened and closed, 4 drafts |
| schema_bump | 3 | 3 | 0 | 0 | 0 | after `accept_schema_v2` |
| mixed_faults seed 7 | 80 | 32 | 33 | 8 | 7 | 32+33+8+7=80 |

Lag seconds, retry sleeps, and processing latency in `report.json` use the simulated clock. The field `clock` is `simulated`.

## Gate detail (same command as the eval row)

```
no_loss: pass=10 fail=0 non_vacuous=10
exactly_once_effect: pass=10 fail=0 non_vacuous=10
per_key_order: pass=10 fail=0 non_vacuous=10
retries_bounded: pass=10 fail=0 non_vacuous=1
poison_isolation: pass=10 fail=0 non_vacuous=4
lag_drains: pass=10 fail=0 non_vacuous=10
agent_budget: pass=10 fail=0 non_vacuous=10
dlq_replay: pass=10 fail=0 non_vacuous=2
violations: 0
```

`non_vacuous` counts scenarios that actually exercised the invariant. A pass with `non_vacuous=0` would only mean the check had nothing to look at. Replay is non-vacuous in 2 scenarios, retries in 1, poison in 4. That is one suite, not repeated experiments.

## Not measured

Live provider latency, token cost, Kafka throughput, Postgres replication lag, and multi-process consumer groups.
