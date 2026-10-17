"""In-memory broker: partitioning, leases, lag, and commit rules."""

from __future__ import annotations

import pytest

from cdc_lab.broker import InMemoryBroker
from cdc_lab.clock import SimulatedClock
from cdc_lab.events import partition_for_key


def test_partitioner_is_stable_and_in_range() -> None:
    first = [partition_for_key(f"shipment:s{i}", 4) for i in range(30)]
    second = [partition_for_key(f"shipment:s{i}", 4) for i in range(30)]
    assert first == second
    assert set(first) <= {0, 1, 2, 3}
    assert len(set(first)) > 1


def test_produce_poll_commit_and_lag() -> None:
    clock = SimulatedClock(1_000)
    broker = InMemoryBroker(partitions=2, visibility_timeout_ms=50, clock=clock)
    first = broker.produce("events", "shipment:a", b"a")
    second = broker.produce("events", "shipment:b", b"b")
    assert first.offset >= 0
    assert partition_for_key("shipment:a", 2) == first.partition
    assert sum(broker.lag("g", "events").values()) == 2
    polled = broker.poll("g", "events", 10)
    assert len(polled) == 2
    assert broker.poll("g", "events", 10) == []
    broker.commit("g", {(first.topic, first.partition): first.offset + 1})
    broker.commit("g", {(second.topic, second.partition): second.offset + 1})
    assert sum(broker.lag("g", "events").values()) == 0
    assert {record.value for record in (first, second)} == {b"a", b"b"}


def test_uncommitted_record_is_redelivered_after_visibility() -> None:
    clock = SimulatedClock(0)
    broker = InMemoryBroker(partitions=1, visibility_timeout_ms=100, clock=clock)
    broker.produce("events", "k", b"v")
    first = broker.poll("g", "events", 1)
    assert len(first) == 1
    assert broker.poll("g", "events", 1) == []
    clock.advance(100)
    assert broker.poll("g", "events", 1) == []
    clock.advance(1)
    again = broker.poll("g", "events", 1)
    assert [record.value for record in again] == [b"v"]


def test_groups_are_independent_and_commits_are_contiguous() -> None:
    clock = SimulatedClock(0)
    broker = InMemoryBroker(partitions=1, visibility_timeout_ms=10, clock=clock)
    broker.produce("events", "k", b"1")
    broker.produce("events", "k", b"2")
    head = broker.poll("g1", "events", 1)[0]
    other = broker.poll("g2", "events", 1)[0]
    assert head.offset == other.offset == 0
    with pytest.raises(ValueError, match="skip"):
        broker.commit("g1", {("events", 0): 2})
    with pytest.raises(ValueError, match="backwards"):
        broker.commit("g1", {("events", 0): -1})
    broker.commit("g1", {("events", 0): 1})
    assert broker.committed_offsets("g2", "events")[0] == 0
    assert broker.read_from("events", 0, 0, 10)[1].value == b"2"
    assert broker.poll("g1", "events", 0) == []
    with pytest.raises(ValueError):
        InMemoryBroker(0, 1, clock)
    with pytest.raises(ValueError):
        broker.read_from("events", 0, -1, 1)
    with pytest.raises(ValueError):
        broker.commit("g1", {("events", 3): 1})


def test_end_offsets_create_the_topic() -> None:
    broker = InMemoryBroker(2, 10, SimulatedClock(0))
    assert broker.end_offsets("fresh") == {0: 0, 1: 0}
