"""Edges: config, YAML, replay profiles, metrics, clocks, and decode faults."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_lab.clock import SimulatedClock, WallClock
from cdc_lab.config import Settings
from cdc_lab.errors import ConfigError
from cdc_lab.events import decode, dumps_event, materialize, project_highest_lsn
from cdc_lab.generate import GenerateConfig, generate
from cdc_lab.invariants import RunTrace, is_non_vacuous
from cdc_lab.metrics import Metrics, Snapshot, percentile, render_prometheus
from cdc_lab.replay import apply_fix
from cdc_lab.scenarios import load_scenarios, scenario_from_mapping
from cdc_lab.stories import _shipment, build_story
from cdc_lab.yamlutil import parse_flat_yaml


def test_settings_from_env_and_validation() -> None:
    settings = Settings.from_env({"CDC_PARTITIONS": "2", "CDC_AGENT_ENABLED": "off"})
    assert settings.partitions == 2
    assert settings.agent_enabled is False
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_PARTITIONS": "nope"})
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_AGENT_ENABLED": "maybe"})
    with pytest.raises(ConfigError):
        Settings(partitions=0).validate()
    with pytest.raises(ConfigError):
        Settings(backoff_cap_ms=1, backoff_base_ms=5).validate()
    with pytest.raises(ConfigError):
        Settings(max_attempts=0).validate()


def test_flat_yaml_and_scenario_errors(tmp_path: Path) -> None:
    parsed = parse_flat_yaml(
        "name: demo\n# comment\nflag: true\nempty: null\nnote: 'x'\n"
    )
    assert parsed["flag"] is True
    assert parsed["empty"] is None
    assert parsed["note"] == "x"
    with pytest.raises(ValueError, match="flat"):
        parse_flat_yaml("  nested: 1\n")
    with pytest.raises(ValueError, match="duplicate"):
        parse_flat_yaml("a: 1\na: 2\n")
    with pytest.raises(ValueError, match="expected"):
        parse_flat_yaml("nope\n")
    with pytest.raises(ConfigError, match="unknown"):
        scenario_from_mapping({"name": "n", "builder": "b", "typo": 1})
    with pytest.raises(ConfigError, match="requires"):
        scenario_from_mapping({"name": "n"})
    with pytest.raises(ConfigError, match="integer"):
        scenario_from_mapping({"name": "n", "builder": "b", "seed": "seven"})
    with pytest.raises(ConfigError, match="no scenarios"):
        load_scenarios(tmp_path)
    with pytest.raises(ValueError, match="unknown story"):
        build_story("missing", object())


def test_replay_profiles_and_generator_guards() -> None:
    raw = dumps_event(_shipment(lsn=1, sid="s", customer_id="c", status="created"))
    assert apply_fix("identity", raw) == raw
    bumped = raw.replace(b'"schema_version":1', b'"schema_version":2')
    fixed = apply_fix("accept_schema_v2", bumped)
    assert fixed is not None and b'"schema_version":1' in fixed
    assert apply_fix("accept_schema_v2", raw) is None
    assert apply_fix("accept_schema_v2", b"\xff") is None
    with pytest.raises(ConfigError, match="unknown fix"):
        apply_fix("nope", raw)
    with pytest.raises(ValueError):
        generate(GenerateConfig(events=0))
    with pytest.raises(ValueError):
        generate(GenerateConfig(customers=0))


def test_metrics_alert_prometheus_and_clocks() -> None:
    clock = SimulatedClock(0)
    metrics = Metrics(clock, lag_threshold=1, lag_hold_ms=100)
    metrics.observe_lag({0: 2, 1: 0})
    clock.advance(100)
    metrics.observe_lag({0: 2, 1: 0})
    assert metrics.alerted is True
    assert metrics.lag_max == 2
    clock.advance(1)
    metrics.observe_lag({0: 0, 1: 0})
    assert metrics.lag_final == 0
    assert percentile([], 95) is None
    snapshot = Snapshot(
        per_partition_lag={1: 0, 0: 2},
        lag_total=2,
        applied=1,
        skipped_stale=0,
        duplicates_skipped=0,
        dlq=0,
        retries=1,
        agent_invocations=0,
        latencies_ms=[4, 40],
        alerted=True,
        lag_max=2,
        latency_p50_ms=4,
        latency_p95_ms=40,
    )
    text = render_prometheus(snapshot)
    assert 'cdc_processing_latency_ms_bucket{le="+Inf"} 2' in text
    assert "cdc_retries_total 1" in text
    wall = WallClock()
    before = wall.now_ms()
    wall.advance(0)
    assert wall.now_ms() >= before
    with pytest.raises(ValueError):
        wall.advance(-1)
    with pytest.raises(ValueError):
        SimulatedClock().advance(-1)


def test_decode_table_rules_and_projection() -> None:
    event = _shipment(lsn=3, sid="s", customer_id="c", status="delayed", op="u")
    image = materialize(event)
    assert image["deleted"] is False
    deleted = _shipment(
        lsn=4,
        sid="s",
        customer_id="c",
        status="cancelled",
        op="d",
        before_status="delayed",
    )
    assert materialize(deleted)["deleted"] is True
    projected = project_highest_lsn([event, event, deleted])
    assert projected["shipment:s"]["lsn"] == 4
    bad_status = dumps_event(event).replace(b'"status":"delayed"', b'"status":"lost"')
    assert decode(bad_status, 8192)[1] is not None
    unknown_table = dumps_event(event).replace(
        b'"table":"shipments"', b'"table":"orders"'
    )
    problem = decode(unknown_table, 8192)[1]
    assert problem is not None and problem.error_code == "schema"
    version = dumps_event(event).replace(
        b'"schema_version":1', b'"schema_version":true'
    )
    assert decode(version, 8192)[1] is not None


def test_non_vacuous_labels() -> None:
    from tests.test_invariants import _good

    trace = _good()
    assert is_non_vacuous("poison_isolation", trace) is True
    empty = RunTrace(
        produced=0,
        applied=0,
        skipped_stale=0,
        duplicates_skipped=0,
        dlq=0,
        effect_counts={},
        final_state={},
        expected_state={},
        attempt_counts=[],
        backoff_ms=[],
        max_attempts=5,
        backoff_cap_ms=100,
        poison=[],
        other_keys_blocked=False,
        lag_final=0,
        lag_alert=False,
        agent_budget=1,
        agent_peak_in_window=0,
        agent_event_ids=[],
        unapproved_sends=0,
        replay_performed=False,
        replay_applies={},
        replay_expected=[],
    )
    assert is_non_vacuous("poison_isolation", empty) is False
    assert is_non_vacuous("dlq_replay", empty) is False
    assert is_non_vacuous("retries_bounded", empty) is False
    with pytest.raises(ValueError):
        from cdc_lab.evalgate import assert_trace_shape

        assert_trace_shape(RunTrace(**{**empty.__dict__, "produced": -1}))
