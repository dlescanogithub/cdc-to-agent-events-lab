"""JSONL run trace. One event per line: run_id, ts, kind, payload."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TextIO

from cdc_lab.clock import Clock
from cdc_lab.logging_json import redact_payload


class TraceWriter:
    """Append-only trace. Payloads are redacted before they are stored."""

    def __init__(self, clock: Clock, sink: TextIO | None = None) -> None:
        self._clock = clock
        self._sink = sink
        self.lines: list[str] = []

    def emit(self, run_id: str, kind: str, payload: Mapping[str, object]) -> None:
        event = {
            "run_id": run_id,
            "ts": self._clock.now_ms(),
            "kind": kind,
            "payload": redact_payload(payload),
        }
        line = json.dumps(event, default=str, sort_keys=True)
        self.lines.append(line)
        if self._sink is not None:
            self._sink.write(line + "\n")
            self._sink.flush()
