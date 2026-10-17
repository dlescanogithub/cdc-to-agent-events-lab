# Scope

## In this repository

- Debezium-shaped envelopes for `shipments` and `customers`, schema version 1.
- Seeded generator with labelled faults.
- In-memory broker: partitions, consumer groups, visibility timeout, at-least-once, lag.
- Optional `KafkaBroker` behind `pip install 'cdc-lab[kafka]'`. Lazy import. No exactly-once transactions.
- Consumer with SQLite or memory idempotency, per-key LSN ordering, bounded retries, full jitter, DLQ.
- Crash points before the store commit and after the store commit but before the offset commit.
- Stub agent: rules, cassette, coalesce, budget, breaker, kill switch, human approval.
- Metrics as JSON and Prometheus text. No metrics server.
- Replay from the DLQ with a named fix profile, and replay from a saved offset.
- Invariant gate and a forced regression.

## Design only

- Real logical decoding and a running Debezium connector.
- Kafka transactional exactly-once.
- A schema registry and compatibility checks beyond version 1 vs everything else.
- Multi-region replication and failover.
- A live model, tool use against a network, or sending a notice.
- Kubernetes, autoscaling, and a hosted control plane.

## Explicitly not claimed

- Throughput of Kafka or Redpanda.
- Latency percentiles of a real provider. Cassette token counts are 0 by construction.
- A comparison against another product. The regression compares the same harness with idempotency off, which shows the gate can fail.
