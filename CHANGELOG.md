# Changelog

## 2026-10-22

Documentation only. The package version stays 0.1.0.

- Expanded the English README: start here, quickstart, the seeded results, use cases and governance, a short business-value section, the measured test run, and what the lab does not prove.
- Added `docs/BUSINESS_VALUE.md`. Money figures are the existing 48 USD estimate and the measured zero software bill. No new prices.
- Updated `docs/HOW_IT_WORKS.md` and `docs/METRICS.md` with the 2026-10-22 pytest run, and with the distinction between `cdc_lab demo` and `scenarios/mixed_faults.yaml`.

## 0.1.0 — 2026-10-17

- Synthetic change-event generator, in-memory at-least-once broker, SQLite idempotency store, dead-letter queue, and stub agent drafts.
- Invariant gate over ten scenarios, plus a forced regression that exits 1 when idempotency is off.
- Optional Kafka adapter (not started by CI) and a Dockerfile that CI does not build.
