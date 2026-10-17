"""Debezium-like change events for the fictional shipper Ruta Sur Logística."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from cdc_lab.errors import PermanentError

SUPPORTED_VERSION = 1
SHIPMENT_STATUSES = frozenset(
    {"created", "in_transit", "delayed", "delivered", "cancelled"}
)
CUSTOMER_TIERS = frozenset({"standard", "premium"})
OPS = frozenset({"c", "u", "d", "r"})


@dataclass(frozen=True)
class EventSource:
    db: str
    schema_name: str
    table: str
    lsn: int
    txid: int
    ts_ms: int

    def to_obj(self) -> dict[str, object]:
        return {
            "db": self.db,
            "schema": self.schema_name,
            "table": self.table,
            "lsn": self.lsn,
            "txid": self.txid,
            "ts_ms": self.ts_ms,
        }


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    source: EventSource
    op: str
    key: str
    before: dict[str, object] | None
    after: dict[str, object] | None
    schema_version: int

    def to_obj(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "source": self.source.to_obj(),
            "op": self.op,
            "key": self.key,
            "before": self.before,
            "after": self.after,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class DecodeProblem:
    """Permanent decode failure. `error_code` is the DLQ reason."""

    error_code: str
    message: str


def dumps_event(event: ChangeEvent) -> bytes:
    return json.dumps(event.to_obj(), sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def partition_for_key(key: str, partitions: int) -> int:
    """Stable partition. Python's built-in hash is not used (it is salted)."""
    if partitions <= 0:
        raise ValueError("partitions must be positive")
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partitions


def materialize(event: ChangeEvent) -> dict[str, object]:
    """Full image after applying one event. The highest LSN wins per key."""
    if event.op == "d":
        return {
            "deleted": True,
            "key": event.key,
            "lsn": event.source.lsn,
            "op": "d",
            "table": event.source.table,
        }
    return {
        "after": event.after,
        "deleted": False,
        "key": event.key,
        "lsn": event.source.lsn,
        "op": event.op,
        "table": event.source.table,
    }


def project_highest_lsn(events: list[ChangeEvent]) -> dict[str, dict[str, object]]:
    """State of the highest LSN per key. Duplicate event ids collapse first."""
    by_id: dict[str, ChangeEvent] = {}
    for event in events:
        by_id.setdefault(event.event_id, event)
    best: dict[str, ChangeEvent] = {}
    for event in by_id.values():
        current = best.get(event.key)
        if current is None or event.source.lsn > current.source.lsn:
            best[event.key] = event
    return {key: materialize(event) for key, event in sorted(best.items())}


def decode(
    raw: bytes, max_bytes: int
) -> tuple[ChangeEvent | None, DecodeProblem | None]:
    """Classify bytes. Oversized is checked before JSON parsing."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if len(raw) > max_bytes:
        return None, DecodeProblem("oversized", f"payload is {len(raw)} bytes")
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, DecodeProblem("malformed", "payload is not JSON")
    if not isinstance(parsed, dict):
        return None, DecodeProblem("schema", "envelope must be an object")
    problem = _structural(parsed)
    if problem is not None:
        return None, problem
    version = parsed["schema_version"]
    if not isinstance(version, int) or isinstance(version, bool):
        return None, DecodeProblem("schema", "schema_version must be an integer")
    if version != SUPPORTED_VERSION:
        return None, DecodeProblem(
            "unknown_schema", f"unsupported schema_version {version}"
        )
    try:
        event = _build(parsed)
    except PermanentError as exc:
        return None, DecodeProblem("schema", str(exc))
    table_problem = _validate_table(event)
    if table_problem is not None:
        return None, table_problem
    return event, None


def _structural(parsed: Mapping[str, object]) -> DecodeProblem | None:
    required = ("event_id", "source", "op", "key", "before", "after", "schema_version")
    for field in required:
        if field not in parsed:
            return DecodeProblem("schema", f"missing {field}")
    source = parsed["source"]
    if not isinstance(source, dict):
        return DecodeProblem("schema", "source must be an object")
    for field in ("db", "schema", "table", "lsn", "txid", "ts_ms"):
        if field not in source:
            return DecodeProblem("schema", f"missing source.{field}")
    return None


def _as_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PermanentError(f"{field} must be an integer")
    if value < 0:
        raise PermanentError(f"{field} must be >= 0")
    return value


def _as_object(value: object, field: str) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise PermanentError(f"{field} must be an object or null")
    return dict(value)


def _build(parsed: Mapping[str, object]) -> ChangeEvent:
    source_raw = parsed["source"]
    if not isinstance(source_raw, dict):
        raise PermanentError("source must be an object")
    event_id = parsed["event_id"]
    op = parsed["op"]
    key = parsed["key"]
    if not isinstance(event_id, str) or not event_id:
        raise PermanentError("event_id must be a non-empty string")
    if not isinstance(op, str) or op not in OPS:
        raise PermanentError("op must be c, u, d or r")
    if not isinstance(key, str) or not key:
        raise PermanentError("key must be a non-empty string")
    db = source_raw["db"]
    schema_name = source_raw["schema"]
    table = source_raw["table"]
    if not isinstance(db, str) or not db:
        raise PermanentError("source.db must be a non-empty string")
    if not isinstance(schema_name, str) or not schema_name:
        raise PermanentError("source.schema must be a non-empty string")
    if not isinstance(table, str) or not table:
        raise PermanentError("source.table must be a non-empty string")
    return ChangeEvent(
        event_id=event_id,
        source=EventSource(
            db=db,
            schema_name=schema_name,
            table=table,
            lsn=_as_int(source_raw["lsn"], "source.lsn"),
            txid=_as_int(source_raw["txid"], "source.txid"),
            ts_ms=_as_int(source_raw["ts_ms"], "source.ts_ms"),
        ),
        op=op,
        key=key,
        before=_as_object(parsed["before"], "before"),
        after=_as_object(parsed["after"], "after"),
        schema_version=_as_int(parsed["schema_version"], "schema_version"),
    )


def _require_str(row: Mapping[str, object], field: str) -> None:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise PermanentError(f"{field} must be a non-empty string")


def _validate_table(event: ChangeEvent) -> DecodeProblem | None:
    try:
        if event.op == "d":
            if event.after is not None or event.before is None:
                raise PermanentError("delete requires before and null after")
            return None
        if event.after is None:
            raise PermanentError("c/u/r require after")
        if event.op == "u" and event.before is None:
            raise PermanentError("update requires before")
        if event.source.table == "shipments":
            _validate_shipment(event.after)
        elif event.source.table == "customers":
            _validate_customer(event.after)
        else:
            raise PermanentError(f"unknown table {event.source.table}")
    except PermanentError as exc:
        return DecodeProblem("schema", str(exc))
    return None


def _validate_shipment(row: Mapping[str, object]) -> None:
    _require_str(row, "id")
    _require_str(row, "customer_id")
    _require_str(row, "status")
    _require_str(row, "promised_date")
    status = row["status"]
    if status not in SHIPMENT_STATUSES:
        raise PermanentError(f"invalid shipment status {status!r}")


def _validate_customer(row: Mapping[str, object]) -> None:
    _require_str(row, "id")
    _require_str(row, "tier")
    _require_str(row, "locale")
    tier = row["tier"]
    if tier not in CUSTOMER_TIERS:
        raise PermanentError(f"invalid customer tier {tier!r}")
