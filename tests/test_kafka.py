"""Kafka adapter with injected clients. No broker process and no network."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest

from cdc_lab.broker import KafkaBroker, load_confluent
from cdc_lab.events import partition_for_key


class _TopicView:
    topic: str
    partition: int
    offset: int


class _Msg:
    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: bytes,
        value: bytes | None,
        err: object | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._err = err

    def error(self) -> object | None:
        return self._err

    def topic(self) -> str | None:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return (1, 7)


class _Ack:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _Producer:
    def __init__(self) -> None:
        self.logs: dict[tuple[str, int], list[tuple[bytes, bytes]]] = {}
        self.fail: str | None = None
        self.remaining = 0

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        partition: int,
        callback: Callable[[object | None, object], None],
    ) -> None:
        if self.fail:
            callback(self.fail, _Ack(partition, -1))
            return
        log = self.logs.setdefault((topic, partition), [])
        offset = len(log)
        log.append((key, value))
        callback(None, _Ack(partition, offset))

    def flush(self, timeout: float = 10.0) -> int:
        return self.remaining


class _Consumer:
    def __init__(self, producer: _Producer) -> None:
        self.producer = producer
        self.subscribed: list[str] = []
        self.assigned: list[object] = []
        self.position: dict[tuple[str, int], int] = {}
        self.commits: list[object] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def consume(self, num_messages: int, timeout: float) -> list[_Msg]:
        topic = self.subscribed[0]
        out: list[_Msg] = []
        for partition in range(8):
            log = self.producer.logs.get((topic, partition), [])
            cursor = self.position.get((topic, partition), 0)
            while cursor < len(log) and len(out) < num_messages:
                key, value = log[cursor]
                out.append(_Msg(topic, partition, cursor, key, value))
                cursor += 1
            self.position[(topic, partition)] = cursor
            if len(out) >= num_messages:
                break
        if self.assigned:
            part = cast("_TopicView", self.assigned[0])
            topic = part.topic
            partition = part.partition
            start = part.offset
            log = self.producer.logs.get((topic, partition), [])
            out = [
                _Msg(topic, partition, offset, key, value)
                for offset, (key, value) in enumerate(log)
                if offset >= start
            ][:num_messages]
        return out

    def commit(self, *, offsets: list[object], asynchronous: bool) -> None:
        self.commits.extend(offsets)

    def assign(self, partitions: list[object]) -> None:
        self.assigned = list(partitions)

    def seek(self, partition: object) -> None:
        self.assigned = [partition]

    def get_watermark_offsets(
        self, partition: object, timeout: float = 10.0
    ) -> tuple[int, int]:
        part = cast("_TopicView", partition)
        return 0, len(self.producer.logs.get((part.topic, part.partition), []))


def _broker() -> tuple[KafkaBroker, _Producer]:
    producer = _Producer()
    consumers: list[_Consumer] = []

    def factory(group: str) -> _Consumer:
        consumer = _Consumer(producer)
        consumers.append(consumer)
        return consumer

    broker = KafkaBroker(
        "redpanda.invalid:9092",
        partitions=2,
        producer=producer,
        consumer_factory=factory,
    )
    return broker, producer


def test_injected_kafka_produce_poll_commit_and_read() -> None:
    broker, _producer = _broker()
    key = "shipment:demo"
    record = broker.produce("rutasur.cdc.events", key, b'{"ok":true}')
    assert record.partition == partition_for_key(key, 2)
    polled = broker.poll("g", "rutasur.cdc.events", 10)
    assert [item.value for item in polled] == [b'{"ok":true}']
    broker.commit("g", {(record.topic, record.partition): record.offset + 1})
    assert broker.committed_offsets("g", record.topic)[record.partition] == 1
    ends = broker.end_offsets(record.topic)
    assert ends[record.partition] == 1
    again = broker.read_from(record.topic, record.partition, 0, 5)
    assert again[0].offset == 0
    assert broker.poll("g", record.topic, 0) == []
    assert broker.read_from(record.topic, record.partition, 0, 0) == []


def test_kafka_errors_and_skipped_messages() -> None:
    broker, producer = _broker()
    producer.fail = "broker down"
    with pytest.raises(RuntimeError, match="broker down"):
        broker.produce("t", "k", b"v")
    producer.fail = None
    producer.remaining = 1
    with pytest.raises(RuntimeError, match="not acknowledged"):
        broker.produce("t", "k", b"v")
    producer.remaining = 0
    with pytest.raises(RuntimeError, match="has not polled"):
        KafkaBroker(
            "redpanda.invalid:9092",
            1,
            producer=_Producer(),
            consumer_factory=lambda _group: _Consumer(producer),
        ).commit("missing", {("t", 0): 1})
    with pytest.raises(ValueError):
        KafkaBroker(
            "",
            1,
            producer=_Producer(),
            consumer_factory=lambda _g: _Consumer(producer),
        )
    with pytest.raises(ValueError):
        KafkaBroker(
            "h",
            0,
            producer=_Producer(),
            consumer_factory=lambda _g: _Consumer(producer),
        )
    consumer = _Consumer(producer)
    consumer.subscribed = ["t"]
    bad = _Msg("t", 0, 0, b"k", None, err="oops")
    empty = _Msg("t", 0, 1, None, None)
    consumer.consume = lambda _n, _t: [bad, empty]  # type: ignore[method-assign]
    broker._consumers["skip"] = consumer
    broker._subscribed.add(("skip", "t"))
    assert broker.poll("skip", "t", 5) == []


def test_load_confluent_reports_the_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real = builtins.__import__

    def _blocked(name: str, *args: object, **kwargs: object) -> object:
        if name == "confluent_kafka":
            raise ImportError("blocked")
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked)
    with pytest.raises(ImportError, match="cdc-lab\\[kafka\\]"):
        load_confluent()
