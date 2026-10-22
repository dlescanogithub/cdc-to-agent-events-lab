# Cost

No vendor price list is stored in this repository. Unknown money is written as "not measured". Infrastructure guesses are marked [EST].

| Scenario | Model / provider | Tokens or units | Cost / run | p50 / p95 latency | Quality | Price date / source |
| --- | --- | --- | --- | --- | --- | --- |
| Eval gate, 10 scenarios | none (in-process) | 0 | 0 USD, local CPU | not measured (simulated clock) | 0 invariant violations on 2026-10-17, and the same gate text on 2026-10-22 | n/a |
| Forced regression | none | 0 | 0 USD | not measured | gate exit 1, as required, both dates | n/a |
| Stub agent cassette | `stub-cassette`, local text | tokens_in 0, tokens_out 0 by construction | 0 USD | not measured | drafts stay `pending` | script in `cassette.py`, not a price |
| In-memory bench, 1 sample | none | 2000 events | 0 USD | not a percentile; elapsed 17.276627 s | 0 violations | command in METRICS.md, 2026-10-17 |
| Optional compose Postgres + Redpanda | not started | not measured | not measured | not measured | not run | n/a |
| A future live model | not implemented | not measured | not measured | not measured | not measured | no price captured |

## Infra [EST]

A laptop-sized process for this lab is treated as 0 USD marginal cost. A production Kafka cluster, a database, and on-call time are out of scope and are not estimated as a bill of materials here, because no quote was collected.

Measured with real providers: none yet.
