# Provenance

`src/cdc_lab/secretscan.py` is maintained in parallel with the other portfolio repos. The copy in this repository is byte-identical to that shared utility.

secretscan.py sha256: 2cdf0339ef5a85d4f6439e94c973b5858375f9da0e2d38da9b503e06d0f6dff7

The cassette, invariant checker, and JSON trace writer in this repository are small local utilities with the same contracts: a cassette answers `complete(request)` from a hash of the normalized request and errors when the entry is missing; `check(run_trace)` returns a list of violations; traces are JSONL with `run_id`, `ts`, `kind`, and `payload`.

No live provider, broker, or database was used to produce the numbers in the README. Event payloads are synthetic and use the fictional domain `rutasur.example`.
