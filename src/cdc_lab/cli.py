"""Command line. `python -m cdc_lab <subcommand>`."""

from __future__ import annotations

import argparse
import base64
import json
import platform
import random
import sys
import time
from pathlib import Path

from cdc_lab import __version__
from cdc_lab.agent_trigger import AgentTrigger
from cdc_lab.broker import InMemoryBroker, Record
from cdc_lab.clock import SimulatedClock
from cdc_lab.config import Settings
from cdc_lab.consumer import Consumer, Downstream
from cdc_lab.evalgate import run_eval
from cdc_lab.generate import GenerateResult, fit_generate_config, generate
from cdc_lab.logging_json import configure_logging
from cdc_lab.metrics import Metrics
from cdc_lab.pipeline import CONSUMER_NAME, RunResult, persist_run, run_scenario
from cdc_lab.replay import ReplayResult, replay_dlq
from cdc_lab.scenarios import Scenario
from cdc_lab.store import Store
from cdc_lab.topics import GROUP, TOPIC
from cdc_lab.trace import TraceWriter


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc_lab")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    generate_cmd = sub.add_parser("generate", help="write a seeded JSONL stream")
    generate_cmd.add_argument("--seed", type=int, default=7)
    generate_cmd.add_argument("--events", type=int, default=500)
    generate_cmd.add_argument("--out", type=Path)

    run_cmd = sub.add_parser("run", help="generate and consume a faulty stream")
    run_cmd.add_argument("--seed", type=int, default=7)
    run_cmd.add_argument("--events", type=int, default=500)
    run_cmd.add_argument("--workdir", type=Path, default=Path("var/run"))

    sub.add_parser("demo", help="run a small stream and print the accounting identity")

    eval_cmd = sub.add_parser("eval", help="invariant gate over the scenario suite")
    eval_cmd.add_argument(
        "--regression",
        action="store_true",
        help="disable idempotency; the gate must exit 1",
    )

    replay_cmd = sub.add_parser("replay", help="re-drive a saved run")
    replay_cmd.add_argument("--workdir", type=Path, required=True)
    replay_cmd.add_argument("--from-dlq", action="store_true")
    replay_cmd.add_argument("--fix-profile")
    replay_cmd.add_argument("--from-offset", type=int)
    replay_cmd.add_argument("--partition", type=int, default=0)
    replay_cmd.add_argument("--topic", default=TOPIC)

    bench_cmd = sub.add_parser(
        "bench", help="one wall-clock sample of the in-memory path"
    )
    bench_cmd.add_argument("--seed", type=int, default=7)
    bench_cmd.add_argument("--events", type=int, default=2000)

    args = parser.parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    if args.command == "generate":
        return _generate(args.seed, args.events, args.out)
    if args.command == "run":
        return _run(args.seed, args.events, args.workdir, settings)
    if args.command == "demo":
        return _demo()
    if args.command == "eval":
        code, text = run_eval(regression=args.regression)
        sys.stdout.write(text)
        return code
    if args.command == "replay":
        return _replay(args)
    if args.command == "bench":
        return _bench(args.seed, args.events)
    return 2


def _generate(seed: int, events: int, out: Path | None) -> int:
    result = generate(fit_generate_config(seed, events))
    text = _render_generated(result)
    if out is None:
        sys.stdout.write(text)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    return 0


def _render_generated(result: GenerateResult) -> str:
    lines = []
    for record in result.records:
        event_id = None if record.event is None else record.event.event_id
        lines.append(
            json.dumps(
                {
                    "event_id": event_id,
                    "fault": record.fault,
                    "key": record.key,
                    "payload_b64": base64.b64encode(record.payload).decode("ascii"),
                },
                sort_keys=True,
            )
        )
    return "\n".join(lines) + "\n"


def _mixed(seed: int, events: int, settings: Settings) -> Scenario:
    return Scenario(
        name="mixed_faults",
        builder="mixed_faults",
        seed=seed,
        events=events,
        customers=min(24, max(2, events // 15)),
        shipments=min(40, max(2, events // 8)),
        duplicates=min(20, max(0, events // 25)),
        ooo_swaps=min(12, max(0, events // 40)),
        malformed=min(4, max(0, events // 80)),
        schema_violations=min(4, max(0, events // 80)),
        unknown_versions=min(3, max(0, events // 100)),
        oversized=min(2, max(0, events // 200)),
        partitions=settings.partitions,
        agent_budget=max(settings.agent_budget, events),
        agent_window_ms=settings.agent_window_ms,
        agent_cooldown_ms=settings.agent_cooldown_ms,
        agent_breaker_threshold=settings.agent_breaker_threshold,
        coalesce_window_ms=settings.coalesce_window_ms,
        slip_days=settings.date_slip_days,
        agent_enabled=settings.agent_enabled,
        lag_threshold=settings.lag_threshold,
        lag_hold_ms=settings.lag_hold_ms,
        max_attempts=settings.max_attempts,
        backoff_base_ms=settings.backoff_base_ms,
        backoff_cap_ms=settings.backoff_cap_ms,
        visibility_ms=settings.visibility_ms,
        max_bytes=settings.max_payload_bytes,
        batch_size=16,
    )


def _run(seed: int, events: int, workdir: Path, settings: Settings) -> int:
    scenario = _mixed(seed, events, settings)
    result = run_scenario(scenario, sqlite_path=workdir / "cdc.sqlite")
    persist_run(result, workdir)
    sys.stdout.write(_summary(result))
    return 1 if result.violations else 0


def _demo() -> int:
    scenario = _mixed(7, 80, Settings())
    result = run_scenario(scenario)
    sys.stdout.write(_summary(result))
    sys.stdout.write("clock: simulated\n")
    sys.stdout.write("drafts_pending: ")
    sys.stdout.write(
        str(sum(1 for draft in result.drafts if draft.status == "pending"))
    )
    sys.stdout.write("\n")
    return 1 if result.violations else 0


def _summary(result: RunResult) -> str:
    trace = result.trace
    return (
        f"scenario: {result.scenario}\n"
        f"produced: {trace.produced}\n"
        f"applied: {trace.applied}\n"
        f"skipped_stale: {trace.skipped_stale}\n"
        f"duplicates_skipped: {trace.duplicates_skipped}\n"
        f"dlq: {trace.dlq}\n"
        f"violations: {len(result.violations)}\n"
        "latency_clock: simulated\n"
    )


def _replay(args: argparse.Namespace) -> int:
    if args.from_dlq == bool(args.from_offset is not None):
        sys.stderr.write("choose exactly one of --from-dlq or --from-offset\n")
        return 2
    if args.from_dlq:
        if not args.fix_profile:
            sys.stderr.write("--from-dlq requires --fix-profile\n")
            return 2
        result = _replay_dlq(args.workdir, args.fix_profile)
        sys.stdout.write(
            f"replayable: {len(result.expected)}\n"
            "applied_once: "
            f"{sum(1 for count in result.applies.values() if count == 1)}\n"
            f"left_open: {result.left_open}\n"
        )
        return 0
    counts = _replay_offset(args.workdir, args.topic, args.partition, args.from_offset)
    sys.stdout.write(json.dumps(counts, sort_keys=True) + "\n")
    return 0


def _replay_dlq(workdir: Path, profile: str) -> ReplayResult:
    store = Store.sqlite(workdir / "cdc.sqlite")
    try:
        consumer = _consumer_for(store)
        result = replay_dlq(consumer, store, profile, Settings().max_payload_bytes)
    finally:
        store.close()
    return result


def _replay_offset(
    workdir: Path, topic: str, partition: int, offset: int
) -> dict[str, int]:
    store = Store.sqlite(workdir / "cdc.sqlite")
    try:
        consumer = _consumer_for(store)
        counts: dict[str, int] = {}
        for line in (workdir / "log.jsonl").read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            if not isinstance(item, dict):
                continue
            if int(item["partition"]) != partition or int(item["offset"]) < offset:
                continue
            if str(item["topic"]) != topic:
                continue
            record = Record(
                topic=str(item["topic"]),
                partition=int(item["partition"]),
                offset=int(item["offset"]),
                key=str(item["key"]),
                value=base64.b64decode(str(item["payload_b64"])),
                timestamp_ms=0,
            )
            bucket = consumer.handle(record)
            counts[bucket] = counts.get(bucket, 0) + 1
    finally:
        store.close()
    return counts


def _consumer_for(store: Store) -> Consumer:
    clock = SimulatedClock(0)
    settings = Settings()
    broker = InMemoryBroker(settings.partitions, settings.visibility_ms, clock)
    metrics = Metrics(clock, settings.lag_threshold, settings.lag_hold_ms)
    agent = AgentTrigger(
        clock=clock,
        enabled=settings.agent_enabled,
        budget=settings.agent_budget,
        window_ms=settings.agent_window_ms,
        breaker_threshold=settings.agent_breaker_threshold,
        cooldown_ms=settings.agent_cooldown_ms,
        coalesce_window_ms=settings.coalesce_window_ms,
        slip_days=settings.date_slip_days,
    )
    return Consumer(
        name=CONSUMER_NAME,
        group=GROUP,
        topic=TOPIC,
        broker=broker,
        store=store,
        agent=agent,
        metrics=metrics,
        trace=TraceWriter(clock),
        clock=clock,
        rng=random.Random(0),
        run_id="replay",
        max_attempts=settings.max_attempts,
        backoff_base_ms=settings.backoff_base_ms,
        backoff_cap_ms=settings.backoff_cap_ms,
        max_bytes=settings.max_payload_bytes,
        downstream=Downstream({}),
        sleeper=clock.advance,
    )


def _bench(seed: int, events: int) -> int:
    scenario = _mixed(seed, events, Settings())
    started = time.perf_counter()
    result = run_scenario(scenario)
    elapsed = time.perf_counter() - started
    rate = 0.0 if elapsed == 0 else events / elapsed
    payload = {
        "label": "measured, in-memory, not a Kafka benchmark",
        "events": events,
        "elapsed_s": round(elapsed, 6),
        "events_per_s": round(rate, 2),
        "violations": len(result.violations),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sample": 1,
    }
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 1 if result.violations else 0
