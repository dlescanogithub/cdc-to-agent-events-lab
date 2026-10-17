"""Small deterministic streams behind the scenario files."""

from __future__ import annotations

import json
from dataclasses import dataclass

from cdc_lab.consumer import HandlerFault
from cdc_lab.events import ChangeEvent, EventSource, dumps_event, partition_for_key
from cdc_lab.generate import GenerateConfig, GeneratedRecord, generate
from cdc_lab.topics import DB_NAME, SCHEMA_NAME

_TS = 1_700_000_000_000


@dataclass(frozen=True)
class Story:
    records: list[GeneratedRecord]
    faults: dict[str, HandlerFault]


def build_story(builder: str, scenario: object) -> Story:
    fn = BUILDERS.get(builder)
    if fn is None:
        raise ValueError(f"unknown story builder {builder!r}")
    return fn(scenario)


def _source(table: str, lsn: int, ts: int) -> EventSource:
    return EventSource(
        db=DB_NAME, schema_name=SCHEMA_NAME, table=table, lsn=lsn, txid=lsn, ts_ms=ts
    )


def _row(sid: str, customer_id: str, status: str, promised: str) -> dict[str, object]:
    return {
        "id": sid,
        "customer_id": customer_id,
        "status": status,
        "promised_date": promised,
        "origin": "Puerto Alerce",
        "destination": "Valle Seco",
    }


def _shipment(
    *,
    lsn: int,
    sid: str,
    customer_id: str,
    status: str,
    op: str = "c",
    before_status: str | None = None,
    promised: str = "2026-04-01",
    before_promised: str | None = None,
    ts: int | None = None,
) -> ChangeEvent:
    key = f"shipment:{sid}"
    before = None
    if before_status is not None:
        before = _row(sid, customer_id, before_status, before_promised or promised)
    after = None if op == "d" else _row(sid, customer_id, status, promised)
    return ChangeEvent(
        event_id=f"evt-{lsn:06d}",
        source=_source("shipments", lsn, _TS + lsn * 1000 if ts is None else ts),
        op=op,
        key=key,
        before=before,
        after=after,
        schema_version=1,
    )


def _customer(lsn: int, customer_id: str, tier: str) -> ChangeEvent:
    return ChangeEvent(
        event_id=f"evt-{lsn:06d}",
        source=_source("customers", lsn, _TS + lsn * 1000),
        op="c",
        key=f"customer:{customer_id}",
        before=None,
        after={
            "id": customer_id,
            "tier": tier,
            "locale": "es-AR",
            "display_name": f"Cliente {customer_id}",
            "email": f"cliente-{customer_id}@rutasur.example",
        },
        schema_version=1,
    )


def _pack(event: ChangeEvent, fault: str | None = None) -> GeneratedRecord:
    return GeneratedRecord(
        event.key, dumps_event(event), fault, None if fault else event
    )


def _key_for(partition: int, partitions: int, salt: int) -> tuple[str, str]:
    index = 0
    while True:
        sid = f"p{partition}s{salt}-{index}"
        key = f"shipment:{sid}"
        if partition_for_key(key, partitions) == partition:
            return key, sid
        index += 1


def _duplicate_burst(scenario: object) -> Story:
    event = _shipment(lsn=1, sid="s-dup", customer_id="c0001", status="created")
    packed = _pack(event)
    return Story([packed for _ in range(5)], {})


def _out_of_order(scenario: object) -> Story:
    higher = _shipment(
        lsn=2,
        sid="s-ord",
        customer_id="c0001",
        status="delivered",
        op="u",
        before_status="in_transit",
        promised="2026-04-02",
        before_promised="2026-04-01",
    )
    lower = _shipment(
        lsn=1,
        sid="s-ord",
        customer_id="c0001",
        status="in_transit",
        op="u",
        before_status="created",
    )
    return Story([_pack(higher), _pack(lower)], {})


def _poison_middle(scenario: object) -> Story:
    partitions = _need_int(scenario, "partitions")
    _key_a, sid_a = _key_for(0, partitions, 1)
    _key_b, sid_b = _key_for(0, partitions, 2)
    _key_c, sid_c = _key_for(1 if partitions > 1 else 0, partitions, 3)
    poison_key, _poison_sid = _key_for(0, partitions, 9)
    first = _shipment(lsn=1, sid=sid_a, customer_id="c0001", status="created")
    second = _shipment(lsn=2, sid=sid_b, customer_id="c0001", status="created")
    third = _shipment(lsn=3, sid=sid_c, customer_id="c0001", status="created")
    # The event key must be the broker key. Rebuild if the searched key differs.
    first = _with_key(first, f"shipment:{sid_a}")
    second = _with_key(second, f"shipment:{sid_b}")
    third = _with_key(third, f"shipment:{sid_c}")
    poison = GeneratedRecord(poison_key, b"{not-json-middle", "malformed", None)
    return Story([_pack(first), poison, _pack(second), _pack(third)], {})


def _with_key(event: ChangeEvent, key: str) -> ChangeEvent:
    if event.key == key:
        return event
    return ChangeEvent(
        event_id=event.event_id,
        source=event.source,
        op=event.op,
        key=key,
        before=event.before,
        after=event.after,
        schema_version=event.schema_version,
    )


def _transient(scenario: object) -> Story:
    event = _shipment(lsn=1, sid="s-retry", customer_id="c0001", status="created")
    return Story([_pack(event)], {event.event_id: HandlerFault("transient", 2)})


def _permanent(scenario: object) -> Story:
    customer = _customer(1, "c0001", "standard")
    shipment = _shipment(lsn=2, sid="s-perm", customer_id="c0001", status="created")
    return Story(
        [_pack(customer), _pack(shipment)],
        {shipment.event_id: HandlerFault("permanent", 1)},
    )


def _clean(scenario: object, count: int, prefix: str) -> Story:
    records = [
        _pack(
            _shipment(
                lsn=i + 1, sid=f"{prefix}-{i}", customer_id="c0001", status="created"
            )
        )
        for i in range(count)
    ]
    return Story(records, {})


def _crash(scenario: object) -> Story:
    return _clean(scenario, 5, "crash")


def _lag(scenario: object) -> Story:
    return _clean(scenario, _need_int(scenario, "events"), "lag")


def _breaker(scenario: object) -> Story:
    customer = _customer(1, "prem", "premium")
    records = [_pack(customer)]
    for index in range(4):
        event = _shipment(
            lsn=2 + index,
            sid=f"s-brk-{index}",
            customer_id="prem",
            status="delayed",
            op="u",
            before_status="created",
            ts=_TS + (2 + index) * 1000,
        )
        records.append(_pack(event))
    return Story(records, {})


def _schema_bump(scenario: object) -> Story:
    customer = _customer(1, "c0001", "standard")
    base = _shipment(lsn=2, sid="s-ver", customer_id="c0001", status="created")
    upgraded = _shipment(
        lsn=3,
        sid="s-ver",
        customer_id="c0001",
        status="delivered",
        op="u",
        before_status="created",
        promised="2026-04-03",
        before_promised="2026-04-01",
    )
    body = upgraded.to_obj()
    body["schema_version"] = 2
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    bumped = GeneratedRecord(upgraded.key, raw, "unknown_schema", None)
    return Story([_pack(customer), _pack(base), bumped], {})


def _generated(scenario: object) -> Story:
    config = GenerateConfig(
        seed=_need_int(scenario, "seed"),
        events=_need_int(scenario, "events"),
        customers=_need_int(scenario, "customers"),
        shipments=_need_int(scenario, "shipments"),
        duplicates=_need_int(scenario, "duplicates"),
        ooo_swaps=_need_int(scenario, "ooo_swaps"),
        malformed=_need_int(scenario, "malformed"),
        schema_violations=_need_int(scenario, "schema_violations"),
        unknown_versions=_need_int(scenario, "unknown_versions"),
        oversized=_need_int(scenario, "oversized"),
        max_bytes=_need_int(scenario, "max_bytes"),
    )
    return Story(generate(config).records, {})


def _need_int(scenario: object, name: str) -> int:
    value = getattr(scenario, name)
    if not isinstance(value, int):
        raise ValueError(f"{name} must be an int")
    return value


BUILDERS = {
    "duplicate_burst": _duplicate_burst,
    "out_of_order": _out_of_order,
    "poison_middle": _poison_middle,
    "transient_recovery": _transient,
    "permanent_replay": _permanent,
    "crash_after_effect": _crash,
    "lag_drain": _lag,
    "agent_breaker": _breaker,
    "schema_bump": _schema_bump,
    "mixed_faults": _generated,
}
