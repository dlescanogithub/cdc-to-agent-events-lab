# Changelog

## 0.1.1 - 2026-10-22

- Point security reports at GitHub private vulnerability reporting ("Report a vulnerability" on the Security tab). Dropped the old advisory sentence.
- Translated `docs/HOW_IT_WORKS.md` into English. Interview-prep framing now reads as a walkthrough for engineers and buyers, including the audience lines in the README, `docs/BUSINESS_CASE.md`, and `docs/BUSINESS_VALUE.md`.
- Ignore the `secretscan.py` sha256 line in `docs/PROVENANCE.md` with one `.gitleaksignore` fingerprint. Gitleaks reports that hex as `generic-api-key`.
- Expanded the README: start here, quickstart, the seeded results, use cases and governance, a short business-value section, the measured test run, and what the lab does not prove.
- Added `docs/BUSINESS_VALUE.md`. Money figures are the existing 48 USD estimate and the measured zero software bill. No new prices.
- Updated `docs/HOW_IT_WORKS.md` and `docs/METRICS.md` with the 2026-10-22 pytest run, and with the distinction between `cdc_lab demo` and `scenarios/mixed_faults.yaml`.
- Translated the cassette templates, synthetic customer labels, locales, and place names into English. Package version is 0.1.1.

## 0.1.0 — 2026-10-17

- Synthetic change-event generator, in-memory at-least-once broker, SQLite idempotency store, dead-letter queue, and stub agent drafts.
- Invariant gate over ten scenarios, plus a forced regression that exits 1 when idempotency is off.
- Optional Kafka adapter (not started by CI) and a Dockerfile that CI does not build.
