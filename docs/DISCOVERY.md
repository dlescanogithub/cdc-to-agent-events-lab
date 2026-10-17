# Discovery questions

Asked against the fictional shipper, before any real connector exists.

1. Which tables are in the change stream, and who owns the topic name?
2. What is the business key, and is the LSN monotonic per key or only per transaction?
3. Which operations must be applied (insert, update, delete, snapshot read), and which are audit-only?
4. What is a poison message, and who owns the DLQ with what triage time?
5. Is the downstream effect idempotent, or does it send, charge, or page someone?
6. Who may approve a replay, and from which offset or DLQ row?
7. Which shipment changes should draft a notice, and who approves sending it?
8. What is the invocation budget and the circuit-breaker threshold?
9. Which fields are direct identifiers and must stay out of logs?
10. What is the lag threshold, in records and in simulated time, that should raise an alert?

Unanswered for a real deployment: schema registry, multi-region ordering, and a live model. Those stay design-only until someone measures them.
