# Delivery semantics

The lab is at-least-once on the broker and idempotent on the effect. Together that is effectively-once for the state write. It is not a Kafka transaction, and it is not exactly-once for a side effect that leaves the process.

## Crash between effect and offset

```
produce -> poll -> begin tx -> write effect + dedupe -> commit tx
                                     |
                                     X crash before broker.commit
                                     |
                               restart, same offset redelivered
                                     |
                               effect row exists -> do not apply again
                                     |
                               commit offset
```

`before_store_commit` raises inside the transaction, so the dedupe row rolls back and the next attempt applies once.

`after_store_before_offset` commits the store and then crashes. The redelivery sees the account row and returns the original bucket.

## When replay is safe

Safe: the effect is `upsert` of state plus an outbox row keyed by `event_id`.

Not safe: anything that sends, charges, or pages. This repository does not have that side effect. Adding one without an idempotency key would make replay duplicate the action.
