"""Replay DLQ rows after a fix profile, and re-read a log range.

Replay is safe here because the effect is an idempotent state write plus an
outbox row keyed by event id. It is not safe for a downstream that sends,
charges, or otherwise acts outside that transaction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from cdc_lab.consumer import Consumer
from cdc_lab.errors import ConfigError
from cdc_lab.events import ChangeEvent, decode
from cdc_lab.store import DlqRow, Store


@dataclass(frozen=True)
class ReplayResult:
    expected: list[str]
    applies: dict[str, int]
    left_open: int


def apply_fix(profile: str, raw: bytes) -> bytes | None:
    """Return repaired bytes, or None when this profile cannot repair them."""
    if profile == "identity":
        return raw
    if profile == "accept_schema_v2":
        return _accept_schema_v2(raw)
    raise ConfigError(f"unknown fix profile {profile!r}")


def replay_dlq(
    consumer: Consumer, store: Store, profile: str, max_bytes: int
) -> ReplayResult:
    expected: list[str] = []
    applies: dict[str, int] = {}
    for row in store.list_dlq(consumer.name, open_only=True):
        event = _replayable(profile, row, max_bytes)
        if event is None:
            continue
        expected.append(row.record_id)
        applies[row.record_id] = consumer.apply_replay(event, row)
    left = len(store.list_dlq(consumer.name, open_only=True))
    return ReplayResult(expected=expected, applies=applies, left_open=left)


def _replayable(profile: str, row: DlqRow, max_bytes: int) -> ChangeEvent | None:
    fixed = apply_fix(profile, row.original)
    if fixed is None:
        return None
    event, problem = decode(fixed, max_bytes)
    if problem is not None or event is None:
        return None
    return event


def _accept_schema_v2(raw: bytes) -> bytes | None:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict) or parsed.get("schema_version") != 2:
        return None
    parsed["schema_version"] = 1
    return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")
