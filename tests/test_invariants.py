"""Each invariant fails on a hand-built bad trace and passes on a good one."""

from __future__ import annotations

from dataclasses import replace

from cdc_lab.invariants import PoisonCheck, RunTrace, check


def _good() -> RunTrace:
    state = {"shipment:s": {"lsn": 1}}
    return RunTrace(
        produced=2,
        applied=1,
        skipped_stale=0,
        duplicates_skipped=0,
        dlq=1,
        effect_counts={"evt-1": 1},
        final_state=state,
        expected_state=dict(state),
        attempt_counts=[2],
        backoff_ms=[10, 20],
        max_attempts=5,
        backoff_cap_ms=100,
        poison=[PoisonCheck(in_dlq=True, original_preserved=True)],
        other_keys_blocked=False,
        lag_final=0,
        lag_alert=False,
        agent_budget=5,
        agent_peak_in_window=1,
        agent_event_ids=["evt-1"],
        unapproved_sends=0,
        replay_performed=True,
        replay_applies={"rec-1": 1},
        replay_expected=["rec-1"],
    )


def test_good_trace_has_no_violations() -> None:
    assert check(_good()) == []


def test_each_invariant_can_fail() -> None:
    cases = {
        "no_loss": replace(_good(), produced=9),
        "exactly_once_effect": replace(_good(), effect_counts={"evt-1": 2}),
        "per_key_order": replace(_good(), final_state={"shipment:s": {"lsn": 0}}),
        "retries_bounded": replace(_good(), attempt_counts=[9], backoff_ms=[10_000]),
        "poison_isolation": replace(
            _good(),
            poison=[PoisonCheck(in_dlq=False, original_preserved=False)],
            other_keys_blocked=True,
        ),
        "lag_drains": replace(_good(), lag_final=4, lag_alert=False),
        "agent_budget": replace(
            _good(),
            agent_peak_in_window=9,
            agent_event_ids=["evt-1", "evt-1"],
            unapproved_sends=1,
        ),
        "dlq_replay": replace(
            _good(), replay_applies={"rec-1": 2}, replay_expected=["rec-1"]
        ),
    }
    for name, trace in cases.items():
        found = [item.invariant for item in check(trace)]
        assert name in found, found


def test_lag_alert_satisfies_the_drain_invariant() -> None:
    trace = replace(_good(), lag_final=4, lag_alert=True)
    assert [item.invariant for item in check(trace)] == []
