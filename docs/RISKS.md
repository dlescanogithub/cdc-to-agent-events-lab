# Risks

| Risk | Likelihood in this lab | Impact if it were production | Mitigation here | Residual |
| --- | --- | --- | --- | --- |
| Duplicate delivery applies the effect twice | Expected under at-least-once | Double notice or double write | Unique `(consumer, event_id)` in one transaction | Broken if the effect itself is not idempotent |
| Out-of-order LSN overwrites newer state | Injected by the generator | Wrong shipment status | Ignore a lower LSN, count `skipped_stale` | A gap in LSN is not filled; we do not wait forever |
| Poison message blocks a partition | Injected | Lag on every key in that partition | Permanent errors go to the DLQ and the offset commits | A bug that classifies poison as transient can still stall |
| Retry storm | Injected transient faults | Downstream overload | Attempt cap, full jitter, seeded RNG | Cap is per record, not a global rate limit |
| Agent spends without a budget | Story only | Cost and noise | Budget window and circuit breaker; failures are not invocations | Budget is in-process, not a shared counter |
| Draft sent without approval | Guarded in code | Customer-facing mistake | `send` raises `NotApproved` | No production send path exists to audit |
| Replay of a non-idempotent side effect | Documented | Duplicate customer action | Replay is specified as safe only for the state write | A future sender must not reuse this replay blindly |
| Secret in the tree | Low | Credential leak | `secretscan`, pre-commit hook, CI | The scanner is pattern-based |
| Someone reads simulated lag as a SLO | Easy to do | Bad capacity plan | Labels say simulated; README states no broker was measured | The bench number can still be quoted out of context |
