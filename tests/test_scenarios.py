"""Scenario suite: accounting, ordering, poison, lag, breaker, and replay."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from cdc_lab.evalgate import assert_trace_shape, run_eval, scenario_directory
from cdc_lab.pipeline import run_scenario
from cdc_lab.scenarios import load_scenario, load_scenarios


def test_every_shipped_scenario_holds_its_invariants() -> None:
    scenarios = load_scenarios(scenario_directory())
    assert len(scenarios) == 10
    for scenario in scenarios:
        result = run_scenario(scenario)
        assert result.violations == [], scenario.name
        assert_trace_shape(result.trace)
        identity = (
            result.trace.applied
            + result.trace.skipped_stale
            + result.trace.duplicates_skipped
            + result.trace.dlq
        )
        assert identity == result.trace.produced


def test_named_scenario_counters(tmp_path: Path) -> None:
    root = scenario_directory()
    duplicate = run_scenario(load_scenario(root / "duplicate_burst.yaml"))
    assert duplicate.trace.applied == 1
    assert duplicate.trace.duplicates_skipped == 4
    assert duplicate.trace.effect_counts == {"evt-000001": 1}

    ordered = run_scenario(load_scenario(root / "out_of_order.yaml"))
    assert ordered.trace.applied == 1
    assert ordered.trace.skipped_stale == 1
    state = ordered.trace.final_state["shipment:s-ord"]
    assert state["lsn"] == 2

    poison = run_scenario(load_scenario(root / "poison_middle.yaml"))
    assert poison.trace.applied == 3
    assert poison.trace.dlq == 1
    assert poison.trace.other_keys_blocked is False

    transient = run_scenario(load_scenario(root / "transient_recovery.yaml"))
    assert transient.trace.applied == 1
    assert transient.trace.attempt_counts == [2]
    assert transient.trace.backoff_ms
    assert max(transient.trace.backoff_ms) <= transient.trace.backoff_cap_ms

    permanent = run_scenario(load_scenario(root / "permanent_replay.yaml"))
    assert permanent.trace.applied == 2
    assert permanent.trace.replay_applies
    assert all(count == 1 for count in permanent.trace.replay_applies.values())
    assert permanent.replay_left_open == 0

    crashed = run_scenario(
        load_scenario(root / "crash_after_effect.yaml"),
        sqlite_path=tmp_path / "crash.sqlite",
    )
    assert crashed.trace.applied == 5
    assert set(crashed.trace.effect_counts.values()) == {1}

    lag = run_scenario(load_scenario(root / "lag_drain.yaml"))
    assert lag.trace.applied == 20
    assert lag.lag_max > 0
    assert lag.trace.lag_alert is True
    assert lag.trace.lag_final == 0
    assert "cdc_lag_alert_firing 1" in lag.prometheus
    assert 'cdc_consumer_lag_records{partition="0"}' in lag.prometheus
    assert '"clock": "simulated"' in lag.snapshot.to_json()

    breaker = run_scenario(load_scenario(root / "agent_breaker.yaml"))
    assert breaker.agent_opened is True
    assert breaker.agent_closed_again is True
    assert breaker.agent_failures == 3
    assert len(breaker.trace.agent_event_ids) == 4
    assert len(breaker.trace.agent_event_ids) == len(set(breaker.trace.agent_event_ids))
    assert breaker.agent_backlog == 0
    assert all(draft.status == "pending" for draft in breaker.drafts)

    schema = run_scenario(load_scenario(root / "schema_bump.yaml"))
    assert schema.trace.applied == 3
    assert schema.replay_left_open == 0
    delivered = schema.trace.final_state["shipment:s-ver"]
    assert delivered["lsn"] == 3

    mixed = run_scenario(load_scenario(root / "mixed_faults.yaml"))
    assert mixed.trace.produced == 80
    assert mixed.trace.dlq > 0
    assert mixed.trace.skipped_stale > 0
    assert mixed.trace.duplicates_skipped > 0


def test_crash_before_store_commit_applies_once(tmp_path: Path) -> None:
    scenario = replace(
        load_scenario(scenario_directory() / "crash_after_effect.yaml"),
        name="crash_before_store",
        crash_point="before_store_commit",
    )
    memory = run_scenario(scenario)
    sqlite = run_scenario(scenario, sqlite_path=tmp_path / "before.sqlite")
    for result in (memory, sqlite):
        assert result.violations == []
        assert result.trace.applied == 5
        assert set(result.trace.effect_counts.values()) == {1}


def test_gate_and_forced_regression_exit_codes() -> None:
    code, text = run_eval(False)
    assert code == 0
    assert "mode: gate" in text
    assert "scenarios: 10" in text
    assert "violations: 0" in text
    assert "non_vacuous=10" in text
    bad, regression = run_eval(True)
    assert bad == 1
    assert "mode: regression" in regression
    assert "exactly_once_effect: pass=0 fail=1" in regression
    assert "component: consumer idempotency disabled" in regression


def test_replay_without_profile_is_rejected() -> None:
    scenario = replace(
        load_scenario(scenario_directory() / "permanent_replay.yaml"),
        fix_profile=None,
    )
    with pytest.raises(ValueError, match="fix_profile"):
        run_scenario(scenario)
