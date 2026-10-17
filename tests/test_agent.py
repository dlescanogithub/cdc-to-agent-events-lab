"""Stub agent: rules, coalesce, budget, breaker pause, approval, kill switch."""

from __future__ import annotations

from dataclasses import replace

import pytest

from cdc_lab.agent_trigger import AgentTrigger
from cdc_lab.clock import SimulatedClock
from cdc_lab.errors import NotApproved
from cdc_lab.evalgate import scenario_directory
from cdc_lab.pipeline import run_scenario
from cdc_lab.scenarios import load_scenario
from cdc_lab.store import OutboxRow, Store


def _agent(
    clock: SimulatedClock,
    *,
    enabled: bool = True,
    budget: int = 2,
    window_ms: int = 1_000,
    coalesce: int = 500,
    fail_times: int = 0,
    threshold: int = 3,
    cooldown: int = 5_000,
) -> AgentTrigger:
    return AgentTrigger(
        clock=clock,
        enabled=enabled,
        budget=budget,
        window_ms=window_ms,
        breaker_threshold=threshold,
        cooldown_ms=cooldown,
        coalesce_window_ms=coalesce,
        slip_days=2,
        fail_times=fail_times,
    )


def _row(event_id: str, ts: int, key: str = "shipment:s") -> OutboxRow:
    payload = (
        '{"days": "3", "rule": "date_slip", "shipment_id": "s", "status": "in_transit"}'
    )
    return OutboxRow(event_id, key, "date_slip", ts, "pending", None, payload)


def test_coalesce_budget_and_approval() -> None:
    clock = SimulatedClock(0)
    agent = _agent(clock, budget=1, coalesce=1_000)
    store = Store.memory()
    with store.transaction():
        store.insert_outbox("materializer", _row("evt-a", 10))
        store.insert_outbox("materializer", _row("evt-b", 20))
        assert store.insert_outbox("materializer", _row("evt-a", 10)) is False
    invoked = agent.dispatch(store, "materializer")
    assert invoked == 1
    rows = {row.event_id: row for row in store.list_outbox("materializer")}
    assert rows["evt-b"].status == "invoked"
    assert rows["evt-a"].status == "coalesced"
    assert rows["evt-a"].coalesced_into == "evt-b"
    assert agent.backlog(store, "materializer") == 0
    draft = store.get_draft("draft-evt-b")
    assert draft is not None and draft.status == "pending"
    with pytest.raises(NotApproved):
        agent.send(store, "draft-evt-b")
    with pytest.raises(NotApproved):
        agent.approve(store, "draft-evt-b", "")
    agent.approve(store, "draft-evt-b", "ops-demo")
    agent.send(store, "draft-evt-b")
    sent = store.get_draft("draft-evt-b")
    assert sent is not None and sent.status == "sent" and sent.approver == "ops-demo"


def test_budget_defers_and_window_resets() -> None:
    clock = SimulatedClock(0)
    agent = _agent(clock, budget=1, window_ms=100, coalesce=0)
    store = Store.memory()
    with store.transaction():
        store.insert_outbox("materializer", _row("evt-a", 1, "shipment:a"))
        store.insert_outbox("materializer", _row("evt-b", 2, "shipment:b"))
    assert agent.dispatch(store, "materializer") == 1
    assert agent.backlog(store, "materializer") == 1
    assert agent.dispatch(store, "materializer") == 0
    clock.advance(100)
    assert agent.dispatch(store, "materializer") == 1
    assert agent.stats.peak_in_window <= agent.budget


def test_kill_switch_records_without_invoking() -> None:
    scenario = replace(
        load_scenario(scenario_directory() / "agent_breaker.yaml"),
        name="kill_switch",
        agent_enabled=False,
        agent_fail_times=0,
    )
    result = run_scenario(scenario)
    assert result.violations == []
    assert result.trace.agent_event_ids == []
    assert result.drafts == []
    assert result.agent_backlog == 0


def test_disabled_dispatch_is_a_no_op() -> None:
    clock = SimulatedClock(0)
    agent = _agent(clock, enabled=False)
    store = Store.memory()
    with store.transaction():
        store.insert_outbox("materializer", _row("evt-a", 1))
    assert agent.dispatch(store, "materializer") == 0
    assert store.list_outbox("materializer")[0].status == "pending"


def test_date_slip_rule_and_non_trigger() -> None:
    from cdc_lab.stories import _customer, _shipment

    clock = SimulatedClock(0)
    agent = _agent(clock)
    customer = _customer(1, "c1", "standard")
    quiet = _shipment(lsn=2, sid="s", customer_id="c1", status="in_transit", op="u")
    assert agent.plan(quiet, {"after": {"tier": "standard"}}) is None
    slipped = _shipment(
        lsn=3,
        sid="s",
        customer_id="c1",
        status="in_transit",
        op="u",
        before_status="created",
        promised="2026-04-10",
        before_promised="2026-04-01",
    )
    plan = agent.plan(slipped, {"after": {"tier": "standard"}})
    assert plan is not None and plan.rule == "date_slip" and plan.status == "pending"
    deleted = _shipment(
        lsn=4,
        sid="s",
        customer_id="c1",
        status="cancelled",
        op="d",
        before_status="created",
    )
    assert agent.plan(deleted, None) is None
    assert agent.plan(customer, None) is None
    bad_date = _shipment(
        lsn=5,
        sid="s2",
        customer_id="c1",
        status="in_transit",
        op="u",
        before_status="created",
        promised="not-a-date",
        before_promised="also-bad",
    )
    assert agent.plan(bad_date, {"after": {}}) is None
