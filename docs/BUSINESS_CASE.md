# Business case

Audience: engineers and buyers reviewing change-data capture and an agent trigger. The company in the story, Ruta Sur Logistics, is fictional. Mailboxes use `@rutasur.example`.

## The job

Premium customers should see a draft notice when a shipment becomes delayed, or when the promised date slips by more than a configured number of days. A human approves the draft. Nothing is sent by the software.

## ROI [EST]

These figures are estimates for a story, not measurements from a ledger.

| Item | Figure | Basis |
| --- | --- | --- |
| Premium shipments that become delayed in the story | 4 drafts in `agent_breaker` | Scenario file, simulated |
| Support contact avoided if a draft is accurate | 1 contact | Assumption |
| Loaded cost of one support contact | 12 USD | [EST], not a vendor price and not from a contract |
| Value of four accurate drafts | 48 USD | [EST] 4 × 12 |
| Cost of this lab run | 0 USD | No paid API, no broker bill |

A real rollout would replace the 12 USD assumption with the operator's own cost, and would measure how often a reviewer rejects a draft. That measurement does not exist here.

## What would have to be true

- The source database emits an ordered LSN per key.
- The effect (state write, outbox row) is idempotent.
- A named person approves drafts and approves replays.
- The kill switch is tested before any send path exists.

See `SCOPE.md` for the boundary and `COST.md` for what was not priced.
