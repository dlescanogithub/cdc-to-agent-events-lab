"""Wire the in-memory broker, store, consumer, and stub agent for one scenario."""

from __future__ import annotations

import base64
import json
import random
from dataclasses import dataclass
from pathlib import Path

from cdc_lab.agent_trigger import AgentTrigger
from cdc_lab.broker import InMemoryBroker, Record
from cdc_lab.clock import SimulatedClock
from cdc_lab.consumer import Consumer, CrashPlan, Downstream, HandlerFault
from cdc_lab.errors import CrashInjected
from cdc_lab.events import ChangeEvent, decode, project_highest_lsn
from cdc_lab.generate import GeneratedRecord
from cdc_lab.invariants import PoisonCheck, RunTrace, Violation, check
from cdc_lab.metrics import Metrics, Snapshot, percentile, render_prometheus
from cdc_lab.replay import ReplayResult, apply_fix, replay_dlq
from cdc_lab.scenarios import Scenario
from cdc_lab.store import DraftRow, Store
from cdc_lab.stories import Story, build_story
from cdc_lab.topics import GROUP, TOPIC
from cdc_lab.trace import TraceWriter

CONSUMER_NAME = "materializer"


@dataclass(frozen=True)
class BoundRecord:
    record_id: str
    key: str
    payload: bytes
    fault: str | None
    event: ChangeEvent | None


@dataclass
class RunResult:
    scenario: str
    trace: RunTrace
    snapshot: Snapshot
    prometheus: str
    drafts: list[DraftRow]
    violations: list[Violation]
    agent_opened: bool
    agent_closed_again: bool
    agent_failures: int
    agent_backlog: int
    replay_left_open: int
    lag_max: int
    trace_lines: list[str]
    produced: list[BoundRecord]


def run_scenario(
    scenario: Scenario,
    *,
    sqlite_path: Path | None = None,
    story: Story | None = None,
    faults: dict[str, HandlerFault] | None = None,
) -> RunResult:
    """Run one scenario to completion on a simulated clock. No sockets."""
    clock = SimulatedClock(1_700_000_000_000)
    rng = random.Random(scenario.seed)
    broker = InMemoryBroker(scenario.partitions, scenario.visibility_ms, clock)
    store = Store.sqlite(sqlite_path) if sqlite_path is not None else Store.memory()
    try:
        return _run(scenario, clock, rng, broker, store, story=story, faults=faults)
    finally:
        store.close()


def _run(
    scenario: Scenario,
    clock: SimulatedClock,
    rng: random.Random,
    broker: InMemoryBroker,
    store: Store,
    *,
    story: Story | None,
    faults: dict[str, HandlerFault] | None,
) -> RunResult:
    built = story if story is not None else build_story(scenario.builder, scenario)
    handler_faults = built.faults if faults is None else faults
    trace = TraceWriter(clock)
    metrics = Metrics(clock, scenario.lag_threshold, scenario.lag_hold_ms)
    agent = AgentTrigger(
        clock=clock,
        enabled=scenario.agent_enabled,
        budget=scenario.agent_budget,
        window_ms=scenario.agent_window_ms,
        breaker_threshold=scenario.agent_breaker_threshold,
        cooldown_ms=scenario.agent_cooldown_ms,
        coalesce_window_ms=scenario.coalesce_window_ms,
        slip_days=scenario.slip_days,
        fail_times=scenario.agent_fail_times,
    )
    crash = None
    if scenario.crash_point:
        crash = CrashPlan(scenario.crash_point, scenario.crash_times)
    consumer = Consumer(
        name=CONSUMER_NAME,
        group=GROUP,
        topic=TOPIC,
        broker=broker,
        store=store,
        agent=agent,
        metrics=metrics,
        trace=trace,
        clock=clock,
        rng=rng,
        run_id=f"{scenario.name}-{scenario.seed}",
        max_attempts=scenario.max_attempts,
        backoff_base_ms=scenario.backoff_base_ms,
        backoff_cap_ms=scenario.backoff_cap_ms,
        max_bytes=scenario.max_bytes,
        idempotent=scenario.idempotency,
        downstream=Downstream(dict(handler_faults)),
        crash=crash,
        service_ms=scenario.service_ms,
        sleeper=clock.advance,
    )
    bound = _produce(broker, built.records)
    metrics.observe_lag(broker.lag(GROUP, TOPIC))
    _drain(scenario, broker, consumer, clock, metrics)
    replay = ReplayResult(expected=[], applies={}, left_open=0)
    if scenario.replay:
        if scenario.fix_profile is None:
            raise ValueError(f"{scenario.name} sets replay without fix_profile")
        # The repaired handler is healthy. The fault that filled the DLQ stays behind.
        consumer.downstream = Downstream({})
        replay = replay_dlq(consumer, store, scenario.fix_profile, scenario.max_bytes)
    _dispatch(scenario, agent, store, clock)
    run_trace = _trace_from_store(scenario, store, bound, metrics, agent, replay)
    violations = check(run_trace)
    snapshot = _snapshot(store, metrics, agent)
    return RunResult(
        scenario=scenario.name,
        trace=run_trace,
        snapshot=snapshot,
        prometheus=render_prometheus(snapshot),
        drafts=store.list_drafts(),
        violations=violations,
        agent_opened=agent.stats.opened,
        agent_closed_again=agent.stats.closed_again,
        agent_failures=agent.stats.failures,
        agent_backlog=agent.backlog(store, CONSUMER_NAME),
        replay_left_open=replay.left_open,
        lag_max=metrics.lag_max,
        trace_lines=list(trace.lines),
        produced=bound,
    )


def _produce(
    broker: InMemoryBroker, records: list[GeneratedRecord]
) -> list[BoundRecord]:
    bound: list[BoundRecord] = []
    for item in records:
        produced: Record = broker.produce(TOPIC, item.key, item.payload)
        record_id = f"{produced.topic}:{produced.partition}:{produced.offset}"
        event = item.event
        if event is not None and event.key != item.key:
            event = ChangeEvent(
                event_id=event.event_id,
                source=event.source,
                op=event.op,
                key=item.key,
                before=event.before,
                after=event.after,
                schema_version=event.schema_version,
            )
        bound.append(BoundRecord(record_id, item.key, item.payload, item.fault, event))
    return bound


def _drain(
    scenario: Scenario,
    broker: InMemoryBroker,
    consumer: Consumer,
    clock: SimulatedClock,
    metrics: Metrics,
) -> None:
    idle = 0
    steps = 0
    limit = max(50, scenario.events * (scenario.max_attempts + 2) + 200)
    while steps < limit:
        steps += 1
        lag = broker.lag(GROUP, TOPIC)
        metrics.observe_lag(lag)
        if sum(lag.values()) == 0:
            return
        try:
            processed = consumer.process_available(scenario.batch_size)
        except CrashInjected:
            clock.advance(scenario.visibility_ms + 1)
            idle = 0
            continue
        if processed == 0:
            idle += 1
            clock.advance(scenario.visibility_ms + 1)
            if idle > 3:
                return
        else:
            idle = 0
    metrics.observe_lag(broker.lag(GROUP, TOPIC))


def _dispatch(
    scenario: Scenario, agent: AgentTrigger, store: Store, clock: SimulatedClock
) -> None:
    for _ in range(scenario.dispatch_rounds):
        agent.dispatch(store, CONSUMER_NAME)
        if agent.backlog(store, CONSUMER_NAME) == 0:
            return
        clock.advance(scenario.agent_cooldown_ms)


def _trace_from_store(
    scenario: Scenario,
    store: Store,
    bound: list[BoundRecord],
    metrics: Metrics,
    agent: AgentTrigger,
    replay: ReplayResult,
) -> RunTrace:
    accounts = store.accounts(CONSUMER_NAME)
    applied = sum(1 for bucket in accounts.values() if bucket == "applied")
    stale = sum(1 for bucket in accounts.values() if bucket == "skipped_stale")
    duplicates = sum(
        1 for bucket in accounts.values() if bucket == "duplicates_skipped"
    )
    dlq = sum(1 for bucket in accounts.values() if bucket == "dlq")
    effects = store.effects(CONSUMER_NAME)
    effect_counts = {
        row.event_id: row.apply_count for row in effects if row.outcome == "applied"
    }
    attempts = store.all_attempts(CONSUMER_NAME)
    attempt_counts = [row.attempts for row in attempts.values()]
    backoffs = [delay for row in attempts.values() for delay in row.backoffs_ms]
    dlq_rows = {
        row.record_id: row for row in store.list_dlq(CONSUMER_NAME, open_only=False)
    }
    open_dlq = {row.record_id for row in store.list_dlq(CONSUMER_NAME, open_only=True)}
    poison: list[PoisonCheck] = []
    poison_keys: set[str] = set()
    for item in bound:
        structural = item.fault in {
            "malformed",
            "schema",
            "unknown_schema",
            "oversized",
        }
        permanent = (
            item.event is not None and item.record_id in dlq_rows and item.fault is None
        )
        if not structural and not permanent:
            continue
        poison_keys.add(item.key)
        row = dlq_rows.get(item.record_id)
        poison.append(
            PoisonCheck(
                in_dlq=row is not None,
                original_preserved=row is not None and row.original == item.payload,
            )
        )
    other_blocked = any(
        item.record_id not in accounts and item.key not in poison_keys for item in bound
    )
    expected_events: list[ChangeEvent] = []
    for item in bound:
        if item.record_id in open_dlq:
            continue
        if item.event is not None:
            expected_events.append(item.event)
            continue
        if scenario.fix_profile is None or accounts.get(item.record_id) != "applied":
            continue
        fixed = apply_fix(scenario.fix_profile, item.payload)
        if fixed is None:
            continue
        event, problem = decode(fixed, scenario.max_bytes)
        if event is not None and problem is None:
            expected_events.append(event)
    unapproved = sum(
        1
        for draft in store.list_drafts()
        if draft.status == "sent" and not draft.approver
    )
    return RunTrace(
        produced=len(bound),
        applied=applied,
        skipped_stale=stale,
        duplicates_skipped=duplicates,
        dlq=dlq,
        effect_counts=effect_counts,
        final_state=store.all_states(CONSUMER_NAME),
        expected_state=project_highest_lsn(expected_events),
        attempt_counts=attempt_counts,
        backoff_ms=backoffs,
        max_attempts=scenario.max_attempts,
        backoff_cap_ms=scenario.backoff_cap_ms,
        poison=poison,
        other_keys_blocked=other_blocked,
        lag_final=metrics.lag_final,
        lag_alert=metrics.alerted,
        agent_budget=scenario.agent_budget,
        agent_peak_in_window=agent.stats.peak_in_window,
        agent_event_ids=list(agent.stats.event_ids),
        unapproved_sends=unapproved,
        replay_performed=scenario.replay,
        replay_applies=dict(replay.applies),
        replay_expected=list(replay.expected),
    )


def _snapshot(store: Store, metrics: Metrics, agent: AgentTrigger) -> Snapshot:
    accounts = store.accounts(CONSUMER_NAME)
    return Snapshot(
        per_partition_lag=dict(metrics.per_partition_lag),
        lag_total=metrics.lag_final,
        applied=sum(1 for bucket in accounts.values() if bucket == "applied"),
        skipped_stale=sum(
            1 for bucket in accounts.values() if bucket == "skipped_stale"
        ),
        duplicates_skipped=sum(
            1 for bucket in accounts.values() if bucket == "duplicates_skipped"
        ),
        dlq=sum(1 for bucket in accounts.values() if bucket == "dlq"),
        retries=metrics.retries,
        agent_invocations=len(agent.stats.event_ids),
        latencies_ms=list(metrics.latencies_ms),
        alerted=metrics.alerted,
        lag_max=metrics.lag_max,
        latency_p50_ms=percentile(metrics.latencies_ms, 50),
        latency_p95_ms=percentile(metrics.latencies_ms, 95),
    )


def persist_run(result: RunResult, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(result.snapshot.to_json(), encoding="utf-8")
    (directory / "metrics.prom").write_text(result.prometheus, encoding="utf-8")
    (directory / "trace.jsonl").write_text(
        "\n".join(result.trace_lines) + ("\n" if result.trace_lines else ""),
        encoding="utf-8",
    )
    summary = {
        "scenario": result.scenario,
        "violations": [v.message for v in result.violations],
        "produced": result.trace.produced,
        "applied": result.trace.applied,
        "skipped_stale": result.trace.skipped_stale,
        "duplicates_skipped": result.trace.duplicates_skipped,
        "dlq": result.trace.dlq,
        "lag_final_simulated": result.trace.lag_final,
        "lag_max_simulated": result.lag_max,
    }
    (directory / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    log_lines: list[str] = []
    for item in result.produced:
        topic, partition_s, offset_s = item.record_id.split(":")
        log_lines.append(
            json.dumps(
                {
                    "topic": topic,
                    "partition": int(partition_s),
                    "offset": int(offset_s),
                    "key": item.key,
                    "fault": item.fault,
                    "payload_b64": base64.b64encode(item.payload).decode("ascii"),
                },
                sort_keys=True,
            )
        )
    (directory / "log.jsonl").write_text(
        "\n".join(log_lines) + ("\n" if log_lines else ""), encoding="utf-8"
    )
