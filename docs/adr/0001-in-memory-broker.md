# ADR 0001 — In-memory broker as the default

## Status

Accepted.

## Context

The lab needs partitions, consumer groups, redelivery, and lag without a running cluster in CI.

## Decision

`InMemoryBroker` is the default. Keys land on `sha256(key) % N`. A commit may advance the watermark by at most one and may not move backwards. Unacked heads are invisible until the visibility lease ends, then the same offset is delivered again. `KafkaBroker` implements the same methods and imports `confluent-kafka` only when constructed without injected clients.

## Consequences

CI stays socket-free. Lag and latency figures from this broker are simulated or, for the single bench, explicitly not a Kafka result. Exactly-once transactions are not offered.
