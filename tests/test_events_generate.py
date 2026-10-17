"""Event decoding and the seeded generator."""

from __future__ import annotations

import pytest

from cdc_lab.events import decode, dumps_event, partition_for_key
from cdc_lab.generate import GenerateConfig, count_inversions, generate
from cdc_lab.stories import _shipment


def test_round_trip_and_fault_classes() -> None:
    event = _shipment(lsn=1, sid="s1", customer_id="c1", status="created")
    raw = dumps_event(event)
    decoded, problem = decode(raw, 8192)
    assert problem is None
    assert decoded is not None
    assert decoded.event_id == event.event_id
    assert decode(b"\xff", 8192)[1] is not None
    assert decode(b"\xff", 8192)[1].error_code == "malformed"  # type: ignore[union-attr]
    assert decode(b"[]", 8192)[1].error_code == "schema"  # type: ignore[union-attr]
    assert decode(b"x" * 20, 8)[1].error_code == "oversized"  # type: ignore[union-attr]
    broken = raw.replace(b'"schema_version":1', b'"schema_version":2')
    assert decode(broken, 8192)[1].error_code == "unknown_schema"  # type: ignore[union-attr]
    missing = b'{"event_id":"e","op":"c"}'
    missing_problem = decode(missing, 100)[1]
    assert missing_problem is not None
    with pytest.raises(ValueError):
        decode(b"{}", 0)
    with pytest.raises(ValueError):
        partition_for_key("k", 0)


def test_delete_and_bad_status() -> None:
    event = _shipment(
        lsn=4,
        sid="s1",
        customer_id="c1",
        status="cancelled",
        op="d",
        before_status="created",
    )
    decoded, problem = decode(dumps_event(event), 8192)
    assert problem is None and decoded is not None and decoded.op == "d"
    bad = dumps_event(event).replace(b'"op":"d"', b'"op":"c"')
    # c with null after is a schema error; the delete payload has after null.
    assert decode(bad, 8192)[1] is not None


def test_generator_is_deterministic_and_injects_each_fault() -> None:
    config = GenerateConfig(seed=7, events=500)
    first = generate(config)
    second = generate(config)
    assert [record.payload for record in first.records] == [
        record.payload for record in second.records
    ]
    assert len(first.records) == 500
    faults = {record.fault for record in first.records}
    assert {"malformed", "schema", "unknown_schema", "oversized", "duplicate"} <= faults
    assert count_inversions(first.records) > 0
    with pytest.raises(ValueError):
        generate(GenerateConfig(events=5, duplicates=5, malformed=1))
