"""JSON logs: ts, level, msg, run_id, plus extras. Secrets are redacted."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from cdc_lab import secretscan

_STANDARD = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "message",
    "asctime",
}

# Database rows may contain these; logs and traces must not.
_OMITTED_KEYS = frozenset(
    {"email", "phone", "name", "display_name", "address", "api_key", "authorization"}
)


def redact_text(text: str) -> str:
    """Replace any substring that matches the vendored secret patterns."""
    redacted = text
    for name, pattern in secretscan.PATTERNS:
        redacted = pattern.sub(f"[REDACTED:{name}]", redacted)
    return redacted


def redact_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Return a JSON-safe copy with secrets redacted and direct identifiers omitted."""
    return {key: _redact_value(key, value) for key, value in payload.items()}


def _redact_value(key: str, value: object) -> object:
    if key.lower() in _OMITTED_KEYS:
        return "[omitted]"
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return redact_payload(value)
    if isinstance(value, list):
        redacted_items: list[object] = []
        for item in value:
            if isinstance(item, str):
                redacted_items.append(redact_text(item))
            elif isinstance(item, Mapping):
                redacted_items.append(redact_payload(item))
            else:
                redacted_items.append(item)
        return redacted_items
    return value


class JsonFormatter(logging.Formatter):
    """One JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "msg": redact_text(record.getMessage()),
            "run_id": getattr(record, "run_id", None),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD or key == "run_id" or key.startswith("_"):
                continue
            payload[key] = _redact_value(key, value)
        return json.dumps(payload, default=str, sort_keys=True)


def configure_logging(level: str = "INFO") -> None:
    """Attach the JSON formatter to the `cdc_lab` logger."""
    logger = logging.getLogger("cdc_lab")
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
