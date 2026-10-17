# ADR 0002 — Idempotency and offset order

## Status

Accepted.

## Context

At-least-once delivery will present the same record, and the generator also emits distinct offsets that share an `event_id`.

## Decision

The store keys effects by `(consumer, event_id)` and accounts by record id `topic:partition:offset`. Effect and dedupe commit in one transaction. The broker offset commits only after that transaction commits. A lower LSN for a key is stored as `stale` and does not change materialized state. The regression build sets `idempotent=False` so the gate can show a double apply.

## Consequences

Crashes are recoverable for this effect. A downstream that is not idempotent is outside the guarantee. SQLite uses WAL and a thread-owned transaction so concurrent `apply_once` calls serialize.
