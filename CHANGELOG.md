# Changelog

## 0.1.0 — 2026-10-17

- Synthetic change-event generator, in-memory at-least-once broker, SQLite idempotency store, dead-letter queue, and stub agent drafts.
- Invariant gate over ten scenarios, plus a forced regression that exits 1 when idempotency is off.
- Optional Kafka adapter (not started by CI) and a Dockerfile that CI does not build.
