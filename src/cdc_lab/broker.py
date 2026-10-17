"""Broker protocol, in-memory at-least-once log, and an optional Kafka adapter.

The in-memory broker is the default and the only one CI exercises. Kafka is a
lazy import: constructing it without injected clients imports `confluent_kafka`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, cast

from cdc_lab.clock import Clock
from cdc_lab.events import partition_for_key


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str
    value: bytes
    timestamp_ms: int


class Broker(Protocol):
    def produce(self, topic: str, key: str, value: bytes) -> Record:
        """Append one record and return its assigned offset."""

    def poll(self, group: str, topic: str, max_records: int) -> list[Record]:
        """Lease up to `max_records` heads. Uncommitted heads are redelivered."""

    def commit(self, group: str, offsets: Mapping[tuple[str, int], int]) -> None:
        """Move the per-partition watermark forward. The value is the next offset."""

    def end_offsets(self, topic: str) -> dict[int, int]:
        """Exclusive end offset of each partition."""

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        """Next offset the group has committed, per partition."""

    def read_from(
        self, topic: str, partition: int, offset: int, limit: int
    ) -> list[Record]:
        """Read the log directly. Does not move a consumer group."""


class InMemoryBroker:
    """Topics with N partitions, key-hash routing, and a visibility lease.

    A commit advances a contiguous watermark by at most one. Uncommitted heads
    become visible again once `clock` passes the lease. This is at-least-once:
    a crash before commit redelivers the same offset.
    """

    def __init__(
        self, partitions: int, visibility_timeout_ms: int, clock: Clock
    ) -> None:
        if partitions <= 0:
            raise ValueError("partitions must be positive")
        if visibility_timeout_ms < 0:
            raise ValueError("visibility timeout must be non-negative")
        self.partitions = partitions
        self.visibility_ms = visibility_timeout_ms
        self.clock = clock
        self._logs: dict[str, list[list[Record]]] = {}
        self._watermarks: dict[tuple[str, str], list[int]] = {}
        self._leases: dict[tuple[str, str], list[int]] = {}
        self._cursor = 0

    def produce(self, topic: str, key: str, value: bytes) -> Record:
        self._ensure_topic(topic)
        partition = partition_for_key(key, self.partitions)
        log = self._logs[topic][partition]
        record = Record(
            topic=topic,
            partition=partition,
            offset=len(log),
            key=key,
            value=value,
            timestamp_ms=self.clock.now_ms(),
        )
        log.append(record)
        return record

    def poll(self, group: str, topic: str, max_records: int) -> list[Record]:
        if max_records <= 0:
            return []
        self._ensure_topic(topic)
        now = self.clock.now_ms()
        out: list[Record] = []
        for step in range(self.partitions):
            partition = (self._cursor + step) % self.partitions
            record = self._lease_head(group, topic, partition, now)
            if record is not None:
                out.append(record)
            if len(out) >= max_records:
                break
        self._cursor = (self._cursor + 1) % self.partitions
        return out

    def commit(self, group: str, offsets: Mapping[tuple[str, int], int]) -> None:
        for (topic, partition), watermark in sorted(offsets.items()):
            self._ensure_topic(topic)
            self._check_partition(partition)
            current = self._group_watermarks(group, topic)[partition]
            if watermark < current:
                raise ValueError("watermark cannot move backwards")
            if watermark > current + 1:
                raise ValueError("cannot skip uncommitted offsets")
            end = len(self._logs[topic][partition])
            if watermark > end:
                raise ValueError("watermark past end of log")
            self._group_watermarks(group, topic)[partition] = watermark
            self._group_leases(group, topic)[partition] = -1

    def end_offsets(self, topic: str) -> dict[int, int]:
        self._ensure_topic(topic)
        return {p: len(self._logs[topic][p]) for p in range(self.partitions)}

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        self._ensure_topic(topic)
        marks = self._group_watermarks(group, topic)
        return {p: marks[p] for p in range(self.partitions)}

    def read_from(
        self, topic: str, partition: int, offset: int, limit: int
    ) -> list[Record]:
        self._ensure_topic(topic)
        self._check_partition(partition)
        if offset < 0 or limit < 0:
            raise ValueError("offset and limit must be non-negative")
        return list(self._logs[topic][partition][offset : offset + limit])

    def lag(self, group: str, topic: str) -> dict[int, int]:
        end = self.end_offsets(topic)
        committed = self.committed_offsets(group, topic)
        return {p: end[p] - committed[p] for p in end}

    def _lease_head(
        self, group: str, topic: str, partition: int, now: int
    ) -> Record | None:
        log = self._logs[topic][partition]
        watermark = self._group_watermarks(group, topic)[partition]
        if watermark >= len(log):
            return None
        leases = self._group_leases(group, topic)
        granted = leases[partition]
        # `granted < 0` means there is no lease. A lease includes its end millisecond.
        if granted >= 0 and now <= granted + self.visibility_ms:
            return None
        leases[partition] = now
        return log[watermark]

    def _ensure_topic(self, topic: str) -> None:
        if topic not in self._logs:
            self._logs[topic] = [[] for _ in range(self.partitions)]

    def _group_watermarks(self, group: str, topic: str) -> list[int]:
        key = (group, topic)
        if key not in self._watermarks:
            self._watermarks[key] = [0 for _ in range(self.partitions)]
            self._leases[key] = [-1 for _ in range(self.partitions)]
        return self._watermarks[key]

    def _group_leases(self, group: str, topic: str) -> list[int]:
        self._group_watermarks(group, topic)
        return self._leases[(group, topic)]

    def _check_partition(self, partition: int) -> None:
        if partition < 0 or partition >= self.partitions:
            raise ValueError(f"partition {partition} is out of range")


class _KafkaMessage(Protocol):
    def error(self) -> object | None: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def timestamp(self) -> tuple[int, int]: ...


class _KafkaProducer(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        partition: int,
        callback: Callable[[object | None, object], None],
    ) -> None: ...

    def flush(self, timeout: float = 10.0) -> int: ...


class _KafkaConsumer(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...
    def consume(self, num_messages: int, timeout: float) -> list[_KafkaMessage]: ...
    def commit(self, *, offsets: list[object], asynchronous: bool) -> None: ...
    def assign(self, partitions: list[object]) -> None: ...
    def seek(self, partition: object) -> None: ...
    def get_watermark_offsets(
        self, partition: object, timeout: float = 10.0
    ) -> tuple[int, int]: ...


def load_confluent() -> tuple[
    Callable[[Mapping[str, object]], _KafkaConsumer],
    Callable[[Mapping[str, object]], _KafkaProducer],
    Callable[[str, int, int], object],
]:
    """Import confluent-kafka. Tests that inject clients never call this."""
    try:
        from confluent_kafka import Consumer, Producer, TopicPartition
    except ImportError as exc:
        raise ImportError(
            "Kafka support requires pip install 'cdc-lab[kafka]'"
        ) from exc
    return (
        cast(Callable[[Mapping[str, object]], _KafkaConsumer], Consumer),
        cast(Callable[[Mapping[str, object]], _KafkaProducer], Producer),
        cast(Callable[[str, int, int], object], TopicPartition),
    )


class KafkaBroker:
    """Thin adapter. Exactly-once transactions are not implemented (design-only)."""

    def __init__(
        self,
        bootstrap_servers: str,
        partitions: int,
        *,
        producer: _KafkaProducer | None = None,
        consumer_factory: Callable[[str], _KafkaConsumer] | None = None,
        poll_timeout_s: float = 1.0,
    ) -> None:
        if partitions <= 0:
            raise ValueError("partitions must be positive")
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers is required")
        self.partitions = partitions
        self._timeout = poll_timeout_s
        self._topic_partition: Callable[[str, int, int], object] | None = None
        if producer is None or consumer_factory is None:
            consumer_cls, producer_cls, topic_partition = load_confluent()
            self._topic_partition = topic_partition
            if producer is None:
                producer = producer_cls({"bootstrap.servers": bootstrap_servers})
            if consumer_factory is None:

                def consumer_factory(group: str) -> _KafkaConsumer:
                    return consumer_cls(
                        {
                            "bootstrap.servers": bootstrap_servers,
                            "group.id": group,
                            "enable.auto.commit": False,
                            "auto.offset.reset": "earliest",
                        }
                    )

        assert producer is not None
        assert consumer_factory is not None
        self._producer = producer
        self._consumer_factory = consumer_factory
        self._consumers: dict[str, _KafkaConsumer] = {}
        self._subscribed: set[tuple[str, str]] = set()
        self._watermarks: dict[tuple[str, str], dict[int, int]] = {}

    def produce(self, topic: str, key: str, value: bytes) -> Record:
        partition = partition_for_key(key, self.partitions)
        delivered: list[tuple[int, int]] = []
        errors: list[str] = []

        def _on_delivery(err: object | None, msg: object) -> None:
            if err is not None:
                errors.append(str(err))
                return
            part = getattr(msg, "partition", lambda: partition)
            off = getattr(msg, "offset", lambda: -1)
            delivered.append((int(part()), int(off())))

        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=value,
            partition=partition,
            callback=_on_delivery,
        )
        remaining = self._producer.flush(10.0)
        if errors:
            raise RuntimeError(errors[0])
        if remaining or not delivered:
            raise RuntimeError("kafka produce was not acknowledged")
        part, offset = delivered[0]
        return Record(topic, part, offset, key, value, 0)

    def poll(self, group: str, topic: str, max_records: int) -> list[Record]:
        if max_records <= 0:
            return []
        consumer = self._consumer(group, topic)
        messages = consumer.consume(max_records, self._timeout)
        records: list[Record] = []
        for message in messages:
            if message.error() is not None or message.value() is None:
                continue
            key_bytes = message.key() or b""
            _kind, stamp = message.timestamp()
            records.append(
                Record(
                    topic=message.topic() or topic,
                    partition=message.partition(),
                    offset=message.offset(),
                    key=key_bytes.decode("utf-8", errors="replace"),
                    value=message.value() or b"",
                    timestamp_ms=stamp,
                )
            )
        return records

    def commit(self, group: str, offsets: Mapping[tuple[str, int], int]) -> None:
        consumer = self._consumers.get(group)
        if consumer is None:
            raise RuntimeError(f"consumer group {group} has not polled")
        topic_partitions: list[object] = []
        for (topic, partition), watermark in offsets.items():
            topic_partitions.append(self._make_tp(topic, partition, watermark))
            self._watermarks.setdefault((group, topic), {})[partition] = watermark
        consumer.commit(offsets=topic_partitions, asynchronous=False)

    def end_offsets(self, topic: str) -> dict[int, int]:
        consumer = self._consumer("__cdc_lab_meta", topic)
        ends: dict[int, int] = {}
        for partition in range(self.partitions):
            _low, high = consumer.get_watermark_offsets(
                self._make_tp(topic, partition, 0)
            )
            ends[partition] = high
        return ends

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        known = self._watermarks.get((group, topic), {})
        return {p: known.get(p, 0) for p in range(self.partitions)}

    def read_from(
        self, topic: str, partition: int, offset: int, limit: int
    ) -> list[Record]:
        if limit <= 0:
            return []
        consumer = self._consumer(f"__cdc_lab_read_{partition}", topic)
        consumer.assign([self._make_tp(topic, partition, offset)])
        consumer.seek(self._make_tp(topic, partition, offset))
        return self.poll(f"__cdc_lab_read_{partition}", topic, limit)[:limit]

    def _consumer(self, group: str, topic: str) -> _KafkaConsumer:
        consumer = self._consumers.get(group)
        if consumer is None:
            consumer = self._consumer_factory(group)
            self._consumers[group] = consumer
        if (group, topic) not in self._subscribed:
            consumer.subscribe([topic])
            self._subscribed.add((group, topic))
        return consumer

    def _make_tp(self, topic: str, partition: int, offset: int) -> object:
        if self._topic_partition is None:
            # Injected tests do not need a real TopicPartition: a simple namespace works
            # when the fake consumer only stores the object.
            return _TopicPartition(topic, partition, offset)
        return self._topic_partition(topic, partition, offset)


@dataclass(frozen=True)
class _TopicPartition:
    topic: str
    partition: int
    offset: int
