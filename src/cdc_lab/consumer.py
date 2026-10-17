"""At-least-once consumer. Idempotency plus per-key LSN ordering.

Offsets are committed only after the effect and the dedupe mark are durable.
`before_store_commit` rolls the transaction back. `after_store_before_offset`
leaves the effect durable and redelivers the same offset, which must not apply twice.
"""

from __future__ import annotations

import json
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass

from cdc_lab.agent_trigger import AgentTrigger
from cdc_lab.backoff import full_jitter_ms
from cdc_lab.broker import Broker, Record
from cdc_lab.clock import Clock
from cdc_lab.errors import CrashInjected, PermanentError, TransientError
from cdc_lab.events import ChangeEvent, decode, materialize
from cdc_lab.metrics import Metrics
from cdc_lab.store import AttemptRow, DlqRow, EffectRow, OutboxRow, Store
from cdc_lab.topics import DLQ_TOPIC
from cdc_lab.trace import TraceWriter

logger = logging.getLogger("cdc_lab.consumer")


@dataclass
class HandlerFault:
    kind: str
    times: int


class Downstream:
    """Scenario hook. The default is a no-op. External sends do not belong here."""

    def __init__(self, faults: dict[str, HandlerFault] | None = None) -> None:
        self.faults = faults or {}
        self.calls: dict[str, int] = {}

    def check(self, event: ChangeEvent) -> None:
        fault = self.faults.get(event.event_id)
        if fault is None:
            return
        seen = self.calls.get(event.event_id, 0) + 1
        self.calls[event.event_id] = seen
        if seen <= fault.times:
            if fault.kind == "transient":
                raise TransientError(f"downstream timeout for {event.event_id}")
            raise PermanentError(f"downstream rejected {event.event_id}")


@dataclass
class CrashPlan:
    point: str
    times: int = 1

    def maybe(self, point: str) -> None:
        if self.times > 0 and self.point == point:
            self.times -= 1
            raise CrashInjected(point)


class Consumer:
    def __init__(
        self,
        *,
        name: str,
        group: str,
        topic: str,
        broker: Broker,
        store: Store,
        agent: AgentTrigger,
        metrics: Metrics,
        trace: TraceWriter,
        clock: Clock,
        rng: random.Random,
        run_id: str,
        max_attempts: int,
        backoff_base_ms: int,
        backoff_cap_ms: int,
        max_bytes: int,
        idempotent: bool = True,
        downstream: Downstream | None = None,
        crash: CrashPlan | None = None,
        service_ms: int = 0,
        sleeper: Callable[[int], None] | None = None,
    ) -> None:
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self.name = name
        self.group = group
        self.topic = topic
        self.broker = broker
        self.store = store
        self.agent = agent
        self.metrics = metrics
        self.trace = trace
        self.clock = clock
        self.rng = rng
        self.run_id = run_id
        self.max_attempts = max_attempts
        self.backoff_base_ms = backoff_base_ms
        self.backoff_cap_ms = backoff_cap_ms
        self.max_bytes = max_bytes
        self.idempotent = idempotent
        self.downstream = downstream if downstream is not None else Downstream()
        self.crash = crash
        self.service_ms = service_ms
        self._sleeper = sleeper

    def process_available(self, max_records: int) -> int:
        processed = 0
        for _ in range(max_records):
            records = self.broker.poll(self.group, self.topic, 1)
            if not records:
                break
            self.handle_and_commit(records[0])
            processed += 1
        return processed

    def handle_and_commit(self, record: Record) -> str:
        started = self.clock.now_ms()
        if self.service_ms:
            self.clock.advance(self.service_ms)
        bucket = self.handle(record)
        self.broker.commit(
            self.group, {(record.topic, record.partition): record.offset + 1}
        )
        self.metrics.latencies_ms.append(self.clock.now_ms() - started)
        return bucket

    def handle(self, record: Record) -> str:
        record_id = f"{record.topic}:{record.partition}:{record.offset}"
        existing = self.store.get_account(self.name, record_id)
        if existing is not None:
            self._emit("redelivery", {"record_id": record_id, "bucket": existing})
            return existing
        event, problem = decode(record.value, self.max_bytes)
        if problem is not None or event is None:
            code = problem.error_code if problem is not None else "malformed"
            message = problem.message if problem is not None else "undecodable"
            self._write_dlq(
                record,
                record_id,
                error_class="permanent",
                error_code=code,
                message=message,
                attempts=1,
            )
            return "dlq"
        return self._handle_event(record, record_id, event)

    def apply_replay(self, event: ChangeEvent, source: DlqRow) -> int:
        """Apply a repaired DLQ row once. Returns the resulting apply_count (1)."""
        self.downstream.check(event)
        body = _body(event)
        with self.store.transaction():
            prior = self.store.get_effect(self.name, event.event_id)
            if prior is not None:
                return prior.apply_count
            self.store.insert_effect(
                self.name,
                EffectRow(
                    event.event_id,
                    source.record_id,
                    event.key,
                    event.source.lsn,
                    "applied",
                    1,
                    body,
                ),
            )
            self.store.upsert_state(self.name, event.key, event.source.lsn, body)
            self._stage_outbox(event)
            account = self.store.get_account(self.name, source.record_id)
            if account == "dlq":
                self.store.update_account(self.name, source.record_id, "applied")
            self.store.mark_replayed(self.name, source.record_id)
        self._emit(
            "replay_applied",
            {
                "event_id": event.event_id,
                "record_id": source.record_id,
                "lsn": event.source.lsn,
            },
        )
        return 1

    def _handle_event(self, record: Record, record_id: str, event: ChangeEvent) -> str:
        early = self._classify_without_effect(record_id, event)
        if early is not None:
            return early
        try:
            self._run_downstream(record, record_id, event)
        except _Handled as handled:
            return handled.bucket
        with self.store.transaction():
            raced = self._classify_inside(record_id, event)
            if raced is not None:
                return raced
            if (
                self.crash is not None
                and self.crash.point == "before_store_commit"
                and self.crash.times > 0
            ):
                # Stage the dedupe row, then die so the transaction rolls it back.
                self.store.insert_effect(
                    self.name,
                    EffectRow(
                        event.event_id,
                        record_id,
                        event.key,
                        event.source.lsn,
                        "applied",
                        1,
                        _body(event),
                    ),
                )
                self.crash.maybe("before_store_commit")
            self._stage_apply(record_id, event)
        if self.crash is not None:
            self.crash.maybe("after_store_before_offset")
        self._emit(
            "applied",
            {
                "event_id": event.event_id,
                "record_id": record_id,
                "key": event.key,
                "lsn": event.source.lsn,
                "op": event.op,
            },
        )
        return "applied"

    def _classify_without_effect(
        self, record_id: str, event: ChangeEvent
    ) -> str | None:
        with self.store.transaction():
            return self._classify_inside(record_id, event)

    def _classify_inside(self, record_id: str, event: ChangeEvent) -> str | None:
        prior = self.store.get_effect(self.name, event.event_id)
        if prior is not None:
            return self._account_prior(record_id, event, prior)
        last = self.store.last_lsn(self.name, event.key)
        if last is not None and event.source.lsn < last:
            self.store.insert_effect(
                self.name,
                EffectRow(
                    event.event_id,
                    record_id,
                    event.key,
                    event.source.lsn,
                    "stale",
                    0,
                    _body(event),
                ),
            )
            self.store.insert_account(self.name, record_id, "skipped_stale")
            self._emit(
                "skipped_stale",
                {
                    "event_id": event.event_id,
                    "record_id": record_id,
                    "lsn": event.source.lsn,
                },
            )
            return "skipped_stale"
        return None

    def _account_prior(
        self, record_id: str, event: ChangeEvent, prior: EffectRow
    ) -> str:
        if prior.record_id == record_id:
            bucket = "applied" if prior.outcome == "applied" else "skipped_stale"
        elif not self.idempotent and prior.outcome == "applied":
            self.store.bump_effect(
                self.name, event.event_id, _body(event), event.source.lsn
            )
            self.store.upsert_state(
                self.name, event.key, event.source.lsn, _body(event)
            )
            bucket = "applied"
        else:
            bucket = "duplicates_skipped"
        if self.store.get_account(self.name, record_id) is None:
            self.store.insert_account(self.name, record_id, bucket)
        self._emit(
            bucket,
            {
                "event_id": event.event_id,
                "record_id": record_id,
                "lsn": event.source.lsn,
            },
        )
        return bucket

    def _stage_apply(self, record_id: str, event: ChangeEvent) -> None:
        body = _body(event)
        already = self.store.get_effect(self.name, event.event_id)
        if already is None:
            self.store.insert_effect(
                self.name,
                EffectRow(
                    event.event_id,
                    record_id,
                    event.key,
                    event.source.lsn,
                    "applied",
                    1,
                    body,
                ),
            )
        self.store.upsert_state(self.name, event.key, event.source.lsn, body)
        self._stage_outbox(event)
        if self.store.get_account(self.name, record_id) is None:
            self.store.insert_account(self.name, record_id, "applied")

    def _stage_outbox(self, event: ChangeEvent) -> None:
        customer = _customer_state(self.store, self.name, event)
        plan = self.agent.plan(event, customer)
        if plan is None:
            return
        self.store.insert_outbox(
            self.name,
            OutboxRow(
                event_id=event.event_id,
                entity_key=event.key,
                rule=plan.rule,
                ts_ms=plan.ts_ms,
                status=plan.status,
                coalesced_into=None,
                payload_json=plan.payload_json,
            ),
        )

    def _run_downstream(
        self, record: Record, record_id: str, event: ChangeEvent
    ) -> None:
        attempt = self.store.get_attempts(self.name, record_id)
        failures = attempt.attempts
        backoffs = list(attempt.backoffs_ms)
        first = attempt.first_failure_ms
        while True:
            try:
                self.downstream.check(event)
                return
            except TransientError as exc:
                failures += 1
                now = self.clock.now_ms()
                if first is None:
                    first = now
                if failures >= self.max_attempts:
                    self.store.save_attempts(
                        self.name,
                        record_id,
                        AttemptRow(failures, tuple(backoffs), first, now),
                    )
                    self._write_dlq(
                        record,
                        record_id,
                        error_class="retries_exhausted",
                        error_code="transient",
                        message=str(exc),
                        attempts=failures,
                        first_failure_ms=first,
                        last_failure_ms=now,
                    )
                    raise _Handled("dlq") from exc
                delay = full_jitter_ms(
                    self.rng, failures - 1, self.backoff_base_ms, self.backoff_cap_ms
                )
                backoffs.append(delay)
                self.store.save_attempts(
                    self.name,
                    record_id,
                    AttemptRow(failures, tuple(backoffs), first, now),
                )
                self.metrics.retries += 1
                self._sleep(delay)
            except PermanentError as exc:
                now = self.clock.now_ms()
                self._write_dlq(
                    record,
                    record_id,
                    error_class="permanent",
                    error_code="downstream",
                    message=str(exc),
                    attempts=failures + 1,
                    first_failure_ms=now,
                    last_failure_ms=now,
                )
                raise _Handled("dlq") from exc

    def _write_dlq(
        self,
        record: Record,
        record_id: str,
        *,
        error_class: str,
        error_code: str,
        message: str,
        attempts: int,
        first_failure_ms: int | None = None,
        last_failure_ms: int | None = None,
    ) -> None:
        now = self.clock.now_ms()
        first = now if first_failure_ms is None else first_failure_ms
        last = now if last_failure_ms is None else last_failure_ms
        with self.store.transaction():
            if self.store.get_account(self.name, record_id) is None:
                self.store.insert_dlq(
                    self.name,
                    DlqRow(
                        record_id=record_id,
                        original=record.value,
                        error_class=error_class,
                        error_code=error_code,
                        message=message,
                        attempts=attempts,
                        first_failure_ms=first,
                        last_failure_ms=last,
                        source_topic=record.topic,
                        source_partition=record.partition,
                        source_offset=record.offset,
                        replayed=False,
                    ),
                )
                self.store.insert_account(self.name, record_id, "dlq")
        self._publish_dlq(
            record, error_class, error_code, message, attempts, first, last
        )
        self._emit(
            "dlq",
            {
                "record_id": record_id,
                "error_class": error_class,
                "error_code": error_code,
                "attempts": attempts,
            },
        )
        logger.warning(
            "dead letter",
            extra={
                "run_id": self.run_id,
                "record_id": record_id,
                "error_code": error_code,
            },
        )

    def _publish_dlq(
        self,
        record: Record,
        error_class: str,
        error_code: str,
        message: str,
        attempts: int,
        first: int,
        last: int,
    ) -> None:
        envelope = {
            "error_class": error_class,
            "error_code": error_code,
            "message": message,
            "attempts": attempts,
            "first_failure_ms": first,
            "last_failure_ms": last,
            "source_topic": record.topic,
            "source_partition": record.partition,
            "source_offset": record.offset,
            "original_b64_len": len(record.value),
        }
        # The table keeps the original bytes. The topic carries the envelope only.
        self.broker.produce(
            DLQ_TOPIC,
            record.key,
            json.dumps(envelope, sort_keys=True).encode("utf-8"),
        )

    def _sleep(self, delay_ms: int) -> None:
        if callable(self._sleeper):
            self._sleeper(delay_ms)
            return
        self.clock.advance(delay_ms)

    def _emit(self, kind: str, payload: dict[str, object]) -> None:
        self.trace.emit(self.run_id, kind, payload)
        logger.info(kind, extra={"run_id": self.run_id, "kind": kind})


def _body(event: ChangeEvent) -> str:
    return json.dumps(materialize(event), sort_keys=True, separators=(",", ":"))


def _customer_state(
    store: Store, consumer: str, event: ChangeEvent
) -> dict[str, object] | None:
    if event.after is None:
        return None
    customer_id = event.after.get("customer_id")
    if not isinstance(customer_id, str):
        return None
    return store.state_json(consumer, f"customer:{customer_id}")


class _Handled(Exception):
    """Internal: the record was dead-lettered inside the retry loop."""

    def __init__(self, bucket: str) -> None:
        super().__init__(bucket)
        self.bucket = bucket
