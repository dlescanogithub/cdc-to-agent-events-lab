# ADR 0003 — Stub agent, no live model

## Status

Accepted.

## Context

The story needs a trigger when a premium shipment is delayed or a promised date slips. Calling a hosted model would spend money and would make the gate non-deterministic.

## Decision

Rules are local. The provider is a cassette keyed by a hash of the normalized request. A missing entry raises `CassetteMiss`. Drafts are inserted as `pending`. `send` raises `NotApproved` unless an approver was recorded. The kill switch writes `suppressed` and does not call the provider. The breaker opens after K consecutive failures, leaves rows queued, and can close after a cooldown. Token counts on the cassette are 0.

## Consequences

README states "Measured with real providers: none yet". A live model is design-only. Quality of the draft text is the template in the repo, not a scored model output.
