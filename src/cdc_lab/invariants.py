"""Invariant checks over a run trace. Exact tool-call trajectories are not checked."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Violation:
    invariant: str
    message: str


@dataclass(frozen=True)
class PoisonCheck:
    in_dlq: bool
    original_preserved: bool


@dataclass
class RunTrace:
    produced: int
    applied: int
    skipped_stale: int
    duplicates_skipped: int
    dlq: int
    effect_counts: dict[str, int] = field(default_factory=dict)
    final_state: dict[str, dict[str, object]] = field(default_factory=dict)
    expected_state: dict[str, dict[str, object]] = field(default_factory=dict)
    attempt_counts: list[int] = field(default_factory=list)
    backoff_ms: list[int] = field(default_factory=list)
    max_attempts: int = 5
    backoff_cap_ms: int = 5_000
    poison: list[PoisonCheck] = field(default_factory=list)
    other_keys_blocked: bool = False
    lag_final: int = 0
    lag_alert: bool = False
    agent_budget: int = 1
    agent_peak_in_window: int = 0
    agent_event_ids: list[str] = field(default_factory=list)
    unapproved_sends: int = 0
    replay_performed: bool = False
    replay_applies: dict[str, int] = field(default_factory=dict)
    replay_expected: list[str] = field(default_factory=list)


def check(run_trace: RunTrace) -> list[Violation]:
    """Return every violation. An empty list means the invariants hold."""
    violations: list[Violation] = []
    for _name, fn in INVARIANTS:
        violations.extend(fn(run_trace))
    return violations


def check_no_loss(trace: RunTrace) -> list[Violation]:
    accounted = (
        trace.applied + trace.skipped_stale + trace.duplicates_skipped + trace.dlq
    )
    if accounted != trace.produced:
        return [
            Violation(
                "no_loss",
                f"produced {trace.produced} != accounted {accounted} "
                f"(applied {trace.applied}, stale {trace.skipped_stale}, "
                f"duplicates {trace.duplicates_skipped}, dlq {trace.dlq})",
            )
        ]
    return []


def check_exactly_once(trace: RunTrace) -> list[Violation]:
    bad = {
        event_id: count for event_id, count in trace.effect_counts.items() if count > 1
    }
    if bad:
        return [
            Violation("exactly_once_effect", f"effect applied more than once: {bad}")
        ]
    return []


def check_per_key_order(trace: RunTrace) -> list[Violation]:
    if trace.final_state != trace.expected_state:
        return [
            Violation(
                "per_key_order",
                "final state does not match the highest LSN per key",
            )
        ]
    return []


def check_retries_bounded(trace: RunTrace) -> list[Violation]:
    violations: list[Violation] = []
    for attempts in trace.attempt_counts:
        if attempts > trace.max_attempts or attempts < 0:
            violations.append(
                Violation(
                    "retries_bounded",
                    f"attempts {attempts} outside 0..{trace.max_attempts}",
                )
            )
            break
    for delay in trace.backoff_ms:
        if delay < 0 or delay > trace.backoff_cap_ms:
            violations.append(
                Violation(
                    "retries_bounded",
                    f"backoff {delay}ms outside 0..{trace.backoff_cap_ms}",
                )
            )
            break
    return violations


def check_poison_isolation(trace: RunTrace) -> list[Violation]:
    if not trace.poison:
        return []
    violations: list[Violation] = []
    for index, item in enumerate(trace.poison):
        if not item.in_dlq:
            violations.append(
                Violation("poison_isolation", f"poison {index} never reached the DLQ")
            )
        if not item.original_preserved:
            violations.append(
                Violation(
                    "poison_isolation", f"poison {index} lost its original payload"
                )
            )
    if trace.other_keys_blocked:
        violations.append(
            Violation(
                "poison_isolation", "a poison record blocked a record of another key"
            )
        )
    return violations


def check_lag_drains(trace: RunTrace) -> list[Violation]:
    if trace.lag_final == 0 or trace.lag_alert:
        return []
    return [
        Violation(
            "lag_drains",
            f"lag ended at {trace.lag_final} and no lag alert was raised",
        )
    ]


def check_agent_budget(trace: RunTrace) -> list[Violation]:
    violations: list[Violation] = []
    if trace.agent_peak_in_window > trace.agent_budget:
        violations.append(
            Violation(
                "agent_budget",
                "invocations in window "
                f"{trace.agent_peak_in_window} > budget {trace.agent_budget}",
            )
        )
    if len(trace.agent_event_ids) != len(set(trace.agent_event_ids)):
        violations.append(
            Violation("agent_budget", "an event invoked the agent more than once")
        )
    if trace.unapproved_sends > 0:
        violations.append(
            Violation(
                "agent_budget", f"{trace.unapproved_sends} drafts sent without approval"
            )
        )
    return violations


def check_dlq_replay(trace: RunTrace) -> list[Violation]:
    if not trace.replay_performed:
        return []
    violations: list[Violation] = []
    for record_id in trace.replay_expected:
        count = trace.replay_applies.get(record_id, 0)
        if count != 1:
            violations.append(
                Violation("dlq_replay", f"replayable {record_id} applied {count} times")
            )
    for record_id, count in trace.replay_applies.items():
        if count > 1:
            violations.append(
                Violation("dlq_replay", f"replay of {record_id} applied {count} times")
            )
    return violations


INVARIANTS: tuple[tuple[str, Callable[[RunTrace], list[Violation]]], ...] = (
    ("no_loss", check_no_loss),
    ("exactly_once_effect", check_exactly_once),
    ("per_key_order", check_per_key_order),
    ("retries_bounded", check_retries_bounded),
    ("poison_isolation", check_poison_isolation),
    ("lag_drains", check_lag_drains),
    ("agent_budget", check_agent_budget),
    ("dlq_replay", check_dlq_replay),
)


def is_non_vacuous(name: str, trace: RunTrace) -> bool:
    """A vacuous pass did not exercise the interesting case. Callers should say so."""
    if name == "poison_isolation":
        return bool(trace.poison)
    if name == "dlq_replay":
        return trace.replay_performed and bool(trace.replay_expected)
    if name == "retries_bounded":
        return bool(trace.attempt_counts or trace.backoff_ms)
    return True
