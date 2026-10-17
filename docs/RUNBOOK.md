# Runbook

All steps assume the in-memory lab unless a line says otherwise. "Simulated" means the clock in the process, not wall time.

## Lag growing

- Detect: `cdc_consumer_lag_records` or `lag` in the run summary stays above `CDC_LAG_THRESHOLD` for `CDC_LAG_HOLD_MS` simulated milliseconds. An alert row is recorded once.
- Contain: stop the producer in the lab (`generate` is batch, so this is "do not enqueue more"). Do not skip offsets.
- Recover: let the consumer drain. `lag_drain` shows lag rise and then a final lag of 0, with the alert still recorded.

## DLQ growing

- Detect: `cdc_dlq_messages` or `dlq` in the summary increases.
- Contain: leave the row. Do not delete it to make the chart look empty.
- Recover: classify the `error_code`. Replay only with a fix profile after a person approves it.

## Poison message

- Detect: `error_code` of `malformed`, `schema`, `unknown_schema`, or `oversized`.
- Contain: the consumer writes the original bytes and commits the offset so other keys move.
- Recover: fix the producer or apply `accept_schema_v2` when the only problem is version 2 with a version-1 body. Other poison is not auto-repaired.

## Stuck partition

- Detect: one partition's lag stays high while others reach 0, and the head record is not a permanent error.
- Contain: inspect attempts. A transient fault retries up to `CDC_MAX_ATTEMPTS`.
- Recover: if attempts are exhausted the row is on the DLQ. If the process crashed, restart; visibility timeout redelivers the head.

## Schema change

- Detect: `unknown_schema` on the DLQ.
- Contain: version 1 remains the supported contract (`schemas/change_event.schema.json`).
- Recover: ship a decoder change and a fix profile. Do not silently accept unknown versions in the hot path.

## Duplicate storm

- Detect: `duplicates_skipped` climbs, `effect_counts` stay at 1.
- Contain: keep idempotency on. The regression command shows the failure mode when it is off.
- Recover: no replay needed for duplicates that were skipped. Find the producer that double-emitted if this is a real system.

## Agent breaker open

- Detect: stats `opened` and a non-empty pending outbox.
- Contain: the breaker does not drop rows. The kill switch (`CDC_AGENT_ENABLED=0`) records `suppressed` and still consumes.
- Recover: after the cooldown the next dispatch is a trial. Success clears the open flag. Drafts still need approval.

## Replay

- Detect: open DLQ rows you intend to apply.
- Contain: confirm the downstream effect is the state write. If a future worker sends mail, do not replay it with this tool.
- Recover: `python -m cdc_lab replay --workdir DIR --from-dlq --fix-profile identity` or `accept_schema_v2`. Offset replay re-reads `log.jsonl` and the idempotency store skips work already applied.

## Consumer rollback

- Detect: crash between effect and offset (`after_store_before_offset`) or a rolled-back transaction (`before_store_commit`).
- Contain: do not commit offsets from outside the consumer.
- Recover: restart the process. Redelivery hits the same record id. The effect count stays 1.
