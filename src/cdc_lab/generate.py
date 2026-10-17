"""Seeded change-event generator. Faults are injected on purpose and labelled."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, replace

from cdc_lab.events import ChangeEvent, EventSource, dumps_event
from cdc_lab.topics import DB_NAME, SCHEMA_NAME

_STATUSES = ("created", "in_transit", "delayed", "delivered")
_LOCALES = ("es-AR", "es-UY")
_ORIGINS = ("Puerto Alerce", "Valle Seco", "Cerro Demostracion", "Bahia Ficticia")


@dataclass(frozen=True)
class GenerateConfig:
    seed: int = 7
    events: int = 500
    customers: int = 24
    shipments: int = 40
    duplicates: int = 20
    ooo_swaps: int = 12
    malformed: int = 4
    schema_violations: int = 4
    unknown_versions: int = 3
    oversized: int = 2
    max_bytes: int = 8192
    start_ms: int = 1_700_000_000_000


@dataclass(frozen=True)
class GeneratedRecord:
    key: str
    payload: bytes
    fault: str | None
    event: ChangeEvent | None


@dataclass(frozen=True)
class GenerateResult:
    records: list[GeneratedRecord]
    ooo_inversions: int


def fit_generate_config(seed: int, events: int) -> GenerateConfig:
    """Default fault mix, reduced so the faults fit inside `events` records.

    The 500-event defaults are unchanged. Smaller counts keep the same priority
    (duplicates first) and stay deterministic for a seed.
    """
    config = GenerateConfig(seed=seed, events=events)
    fields = (
        "duplicates",
        "malformed",
        "schema_violations",
        "unknown_versions",
        "oversized",
    )
    while True:
        total = (
            config.duplicates
            + config.malformed
            + config.schema_violations
            + config.unknown_versions
            + config.oversized
        )
        if total < config.events:
            break
        name = max(fields, key=lambda field: int(getattr(config, field)))
        current = int(getattr(config, name))
        if current <= 0:
            break
        config = replace(config, **{name: current - 1})
    return replace(
        config,
        customers=min(config.customers, max(1, events)),
        shipments=min(config.shipments, max(1, events)),
        ooo_swaps=min(config.ooo_swaps, max(0, events // 2)),
    )


def generate(config: GenerateConfig) -> GenerateResult:
    """Return exactly `config.events` records, deterministically for `config.seed`."""
    if config.events <= 0:
        raise ValueError("events must be positive")
    fault_total = (
        config.duplicates
        + config.malformed
        + config.schema_violations
        + config.unknown_versions
        + config.oversized
    )
    if fault_total >= config.events:
        raise ValueError("fault slots must leave at least one ordinary record")
    if config.customers <= 0 or config.shipments <= 0:
        raise ValueError("customers and shipments must be positive")

    rng = random.Random(config.seed)
    logical = _logical_events(config, rng)
    _apply_ooo_swaps(logical, rng, config.ooo_swaps)
    records = [
        GeneratedRecord(
            key=event.key, payload=dumps_event(event), fault=None, event=event
        )
        for event in logical
    ]
    _inject_faults(records, rng, config)
    return GenerateResult(records=records, ooo_inversions=count_inversions(records))


def count_inversions(records: list[GeneratedRecord]) -> int:
    """Count valid records whose LSN is below the max already seen for that key."""
    seen: dict[str, int] = {}
    inversions = 0
    for record in records:
        if record.event is None:
            continue
        lsn = record.event.source.lsn
        previous = seen.get(record.key)
        if previous is not None and lsn < previous:
            inversions += 1
        if previous is None or lsn > previous:
            seen[record.key] = lsn
    return inversions


@dataclass
class _Customer:
    id: str
    tier: str
    locale: str


@dataclass
class _Shipment:
    id: str
    customer_id: str
    status: str
    promised_date: str
    origin: str
    destination: str


def _logical_events(config: GenerateConfig, rng: random.Random) -> list[ChangeEvent]:
    customers = [
        _Customer(
            id=f"c{i:04d}",
            tier="premium" if i == 0 or rng.random() < 0.35 else "standard",
            locale=_LOCALES[i % len(_LOCALES)],
        )
        for i in range(config.customers)
    ]
    shipments = [
        _Shipment(
            id=f"s{i:04d}",
            customer_id=customers[i % len(customers)].id,
            status="created",
            promised_date="2026-04-01",
            origin=_ORIGINS[i % len(_ORIGINS)],
            destination=_ORIGINS[(i + 1) % len(_ORIGINS)],
        )
        for i in range(config.shipments)
    ]
    lsn = 1
    ts = config.start_ms
    txid = 1
    events: list[ChangeEvent] = []

    def emit(event: ChangeEvent) -> None:
        nonlocal lsn, ts, txid
        events.append(event)
        lsn += 1
        ts += 1_000
        txid += 1

    for customer in customers:
        emit(_customer_event(customer, lsn, ts, txid, "c", None))
    for shipment in shipments:
        emit(_shipment_event(shipment, lsn, ts, txid, "c", None))

    deleted: set[str] = set()
    guard = 0
    while len(events) < config.events:
        guard += 1
        if guard > config.events * 20:
            raise RuntimeError("generator could not fill the requested event count")
        live = [item for item in shipments if item.id not in deleted]
        if not live:
            raise RuntimeError("generator deleted every shipment")
        shipment = live[rng.randrange(len(live))]
        roll = rng.random()
        if (
            roll < 0.08
            and len(events) < config.events - 1
            and len(deleted) + 1 < len(shipments)
        ):
            before = _shipment_row(shipment)
            shipment.status = "cancelled"
            emit(_shipment_event(shipment, lsn, ts, txid, "d", before))
            deleted.add(shipment.id)
            continue
        if roll < 0.18:
            emit(_shipment_event(shipment, lsn, ts, txid, "r", None))
            continue
        before = _shipment_row(shipment)
        if roll < 0.55:
            shipment.status = _STATUSES[rng.randrange(len(_STATUSES))]
        else:
            day = 1 + rng.randrange(27)
            shipment.promised_date = f"2026-05-{day:02d}"
        emit(_shipment_event(shipment, lsn, ts, txid, "u", before))
    return events[: config.events]


def _source(table: str, lsn: int, ts: int, txid: int) -> EventSource:
    return EventSource(
        db=DB_NAME,
        schema_name=SCHEMA_NAME,
        table=table,
        lsn=lsn,
        txid=txid,
        ts_ms=ts,
    )


def _customer_event(
    customer: _Customer,
    lsn: int,
    ts: int,
    txid: int,
    op: str,
    before: dict[str, object] | None,
) -> ChangeEvent:
    after: dict[str, object] = {
        "id": customer.id,
        "tier": customer.tier,
        "locale": customer.locale,
        "display_name": f"Cliente {customer.id}",
        "email": f"cliente-{customer.id}@rutasur.example",
    }
    return ChangeEvent(
        event_id=f"evt-{lsn:06d}",
        source=_source("customers", lsn, ts, txid),
        op=op,
        key=f"customer:{customer.id}",
        before=before,
        after=after,
        schema_version=1,
    )


def _shipment_row(shipment: _Shipment) -> dict[str, object]:
    return {
        "id": shipment.id,
        "customer_id": shipment.customer_id,
        "status": shipment.status,
        "promised_date": shipment.promised_date,
        "origin": shipment.origin,
        "destination": shipment.destination,
    }


def _shipment_event(
    shipment: _Shipment,
    lsn: int,
    ts: int,
    txid: int,
    op: str,
    before: dict[str, object] | None,
) -> ChangeEvent:
    after = None if op == "d" else _shipment_row(shipment)
    return ChangeEvent(
        event_id=f"evt-{lsn:06d}",
        source=_source("shipments", lsn, ts, txid),
        op=op,
        key=f"shipment:{shipment.id}",
        before=before,
        after=after,
        schema_version=1,
    )


def _apply_ooo_swaps(events: list[ChangeEvent], rng: random.Random, swaps: int) -> None:
    if swaps <= 0:
        return
    positions: dict[str, list[int]] = {}
    for index, event in enumerate(events):
        positions.setdefault(event.key, []).append(index)
    candidates = [key for key, idxs in positions.items() if len(idxs) >= 2]
    if not candidates:
        return
    # One guaranteed inversion when the caller asked for any, then extra random swaps.
    _swap_pair(events, positions[candidates[0]])
    for _ in range(swaps - 1):
        key = candidates[rng.randrange(len(candidates))]
        _swap_pair(events, positions[key])


def _swap_pair(events: list[ChangeEvent], indexes: list[int]) -> None:
    left = indexes[0]
    right = indexes[-1]
    if left == right:
        return
    if events[left].source.lsn < events[right].source.lsn:
        events[left], events[right] = events[right], events[left]


def _inject_faults(
    records: list[GeneratedRecord], rng: random.Random, config: GenerateConfig
) -> None:
    slots = list(range(len(records)))
    rng.shuffle(slots)
    cursor = 0

    def take(count: int) -> list[int]:
        nonlocal cursor
        chosen = slots[cursor : cursor + count]
        cursor += count
        return chosen

    for index in take(config.malformed):
        original = records[index]
        payload = b"{broken-" + str(index).encode("ascii")
        records[index] = GeneratedRecord(original.key, payload, "malformed", None)
    for index in take(config.schema_violations):
        original = records[index]
        records[index] = GeneratedRecord(
            original.key, _schema_violation(original), "schema", None
        )
    for index in take(config.unknown_versions):
        original = records[index]
        records[index] = GeneratedRecord(
            original.key, _unknown_version(original), "unknown_schema", None
        )
    for index in take(config.oversized):
        original = records[index]
        records[index] = GeneratedRecord(
            original.key,
            _oversized(original, config.max_bytes),
            "oversized",
            None,
        )
    targets = take(config.duplicates)
    sources = [index for index in slots[cursor:] if records[index].event is not None]
    if config.duplicates and not sources:
        raise RuntimeError("no valid record left to duplicate")
    for offset, target in enumerate(targets):
        source = records[sources[offset % len(sources)]]
        records[target] = GeneratedRecord(
            source.key, source.payload, "duplicate", source.event
        )


def _loads_obj(record: GeneratedRecord) -> dict[str, object]:
    parsed = json.loads(record.payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise RuntimeError("generator produced a non-object")
    return parsed


def _schema_violation(record: GeneratedRecord) -> bytes:
    obj = _loads_obj(record)
    obj["op"] = "x"
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _unknown_version(record: GeneratedRecord) -> bytes:
    obj = _loads_obj(record)
    obj["schema_version"] = 2
    # Keep a valid v1 body so the accept_schema_v2 replay profile can repair it.
    event = record.event
    if event is not None:
        obj = event.to_obj()
        obj["schema_version"] = 2
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _oversized(record: GeneratedRecord, max_bytes: int) -> bytes:
    event = record.event
    if event is None or event.after is None:
        blob = b"x" * (max_bytes + 32)
        return blob
    grown = dict(event.after)
    grown["note"] = "x" * (max_bytes + 64)
    cloned = replace(event, after=grown)
    return dumps_event(cloned)
