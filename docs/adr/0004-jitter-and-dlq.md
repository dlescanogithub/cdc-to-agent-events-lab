# ADR 0004 — Full jitter, poison to the DLQ

## Status

Accepted.

## Context

Retries need a bound, and a bad record must not freeze other keys.

## Decision

Sleep is `random(0, min(cap, base * 2**attempt))` inclusive, from a seeded RNG, via an injectable sleeper. The sleeper in the lab advances the simulated clock. Permanent decode errors and permanent downstream errors go to the DLQ with the original bytes, the error class, the attempt count, and the source offset. Offsets for those records still commit.

## Consequences

Backoff samples are reproducible for a seed and are not wall-clock measurements. Poison isolation is an invariant with a hand-built failing trace in the tests.
