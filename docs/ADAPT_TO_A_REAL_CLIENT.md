# Adapt this lab to a real client

The names in the tree (`rutasur`, `@rutasur.example`, North Wharf, Dry Valley, Demo Hill, Sample Bay) are fictional. Replace them; do not point the generator at a real customer extract.

1. Keep the envelope fields, or version the schema and add a fix profile. Do not loosen version 1 in place.
2. Swap `InMemoryBroker` for `KafkaBroker` only after you have measured that broker yourself. Delete any throughput number that came from `bench`.
3. Point the idempotency store at a database you operate. Preserve the single transaction around effect and dedupe, and commit the offset only after that commit returns.
4. Replace the cassette with a provider only when you also add a budget you can reconcile, a kill switch you have tested, and an approval record. Until then, leave "Measured with real providers: none yet" in the README.
5. Write a new runbook owner for the DLQ and for replay. The tables in the README are the checklist, not an approval.
6. Re-run `python -m cdc_lab eval` on the synthetic suite so the invariants still hold, then add a separate measurement log for the real path. Do not mix the two in one table.
