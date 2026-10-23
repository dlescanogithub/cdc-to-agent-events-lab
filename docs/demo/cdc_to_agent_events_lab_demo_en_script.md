# Final script used: cdc_to_agent_events_lab_demo_en.mp4 (cdc)

Voice: xAI TTS leo, speed 1.08. English burned-in subtitles.


## A_hook (starts 0.80 s)

- EN: A synthetic change stream for Ruta Sur Logistics, a fictional shipper.

- EN: At-least-once delivery, idempotent state, and a stub agent that only prepares notices. Offline.


## B_demo (starts 13.45 s)

- EN: Mixed faults, seed seven: eighty produced, fifty-nine applied, sixteen stale, three duplicates skipped, two on the dead-letter queue.

- EN: Violations zero. Ten notices still pending human approval. Clock: simulated.


## C_eval (starts 29.06 s)

- EN: Gate on ten scenarios: no-loss, exactly-once effect, order, poison isolation, and the rest. Violations: zero.


## D_regress (starts 38.74 s)

- EN: Disable idempotency on a duplicate burst: event evt-000001 applied five times. Violations one. Exit one. That is the expected red.


## E_close (starts 53.34 s)

- EN: A delayed premium shipment should become one notice, even when the event arrives twice.

- EN: Not covered: Debezium, Postgres, Kafka in C-I, or a real send. Notices stay pending until a named person approves.

- EN: Run python -m cdc_lab demo. Lab path cost: zero.

