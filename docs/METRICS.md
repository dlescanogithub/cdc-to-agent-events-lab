# Metrics

## Measured

| Metric | Value | When | Command |
| --- | --- | --- | --- |
| Pytest | 68 passed, coverage 95.55% | 2026-10-22, Python 3.12.3, pytest 9.1.1, package 0.1.0, commit `e82c512` | `python3 -m pytest --cov=cdc_lab --cov-report=term-missing --cov-fail-under=80` |
| Pytest, earlier row | 68 passed, coverage 95.55% | 2026-10-17, Python 3.12.3, package 0.1.0 | `python3 -m pytest` |
| Eval gate | 10 scenarios, 0 violations, exit 0 | 2026-10-17, and the same gate text again on 2026-10-22 | `python3 -m cdc_lab eval` |
| Forced regression | exit 1, exactly-once fails (`evt-000001` applied 5 times) | 2026-10-17, and the same line again on 2026-10-22 | `python3 -m cdc_lab eval --regression` |
| Secret scan | no findings | 2026-10-22 | `python3 -m cdc_lab.secretscan` |
| In-memory bench, one sample | 2000 events, 17.276627 s, 115.76 events/s, 0 violations | 2026-10-17, Linux 6.12.94+, Python 3.12.3. Not re-run on 2026-10-22 | `python3 -m cdc_lab bench --events 2000 --seed 7` |

The 2026-10-22 coverage figure is the `Total coverage` line pytest-cov printed (95.55%). The table's total column showed 96% for the same 2516 statements and 112 misses. That run also printed a coverage warning that module `cdc_lab` was previously imported but not measured. The 95.55% line is the total that was printed anyway. Fail-under is 80.

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

On 2026-10-22 the same scenario objects were run again in-process. The integers in the table were unchanged. `agent_breaker` again left 4 pending notices. The YAML `mixed_faults` run left 6 pending notices. Those 6 are not an input to the 48 USD estimate, which uses the 4 notices from `agent_breaker` only.

`python3 -m cdc_lab demo` is a different mix, also named `mixed_faults`, seed 7, 80 events, built by the helper in `src/cdc_lab/cli.py`. On 2026-10-22, with the log level at WARNING, it printed produced 80, applied 59, stale 16, duplicates 3, dead-letter 2, violations 0, and 10 pending notices. 59 + 16 + 3 + 2 = 80. Do not paste that row over the YAML row above.

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
