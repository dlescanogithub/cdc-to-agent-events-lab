"""Rule layer and stub agent. Drafts stay pending until a human approves them.

No live model is called. The provider is a cassette. A kill switch records the
would-be trigger and does not invoke the provider. The circuit breaker pauses
dispatch and leaves rows in the outbox; it does not drop them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date

from cdc_lab.cassette import (
    CompletionRequest,
    CompletionResponse,
    Provider,
    default_agent_cassette,
    fill_template,
)
from cdc_lab.clock import Clock
from cdc_lab.errors import AgentFailure, NotApproved
from cdc_lab.events import ChangeEvent
from cdc_lab.store import DraftRow, OutboxRow, Store

_MODEL = "stub-cassette"
_SYSTEM = "Draft a notification template. Do not send it."


@dataclass(frozen=True)
class OutboxPlan:
    rule: str
    status: str
    ts_ms: int
    payload_json: str


@dataclass
class AgentStats:
    event_ids: list[str] = field(default_factory=list)
    peak_in_window: int = 0
    failures: int = 0
    opened: bool = False
    closed_again: bool = False
    backlog_peak: int = 0


class FlakyProvider:
    """Fail the first `fail_times` calls, then delegate to the inner provider."""

    def __init__(self, inner: Provider, fail_times: int) -> None:
        self.inner = inner
        self.fail_times = fail_times
        self.calls = 0

    def complete(self, request: CompletionRequest) -> CompletionResponse:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise AgentFailure("stub agent failure")
        return self.inner.complete(request)


class AgentTrigger:
    def __init__(
        self,
        *,
        clock: Clock,
        enabled: bool,
        budget: int,
        window_ms: int,
        breaker_threshold: int,
        cooldown_ms: int,
        coalesce_window_ms: int,
        slip_days: int,
        provider: Provider | None = None,
        fail_times: int = 0,
    ) -> None:
        self.clock = clock
        self.enabled = enabled
        self.budget = budget
        self.window_ms = window_ms
        self.breaker_threshold = breaker_threshold
        self.cooldown_ms = cooldown_ms
        self.coalesce_window_ms = coalesce_window_ms
        self.slip_days = slip_days
        inner = provider if provider is not None else default_agent_cassette()
        self.provider: Provider | FlakyProvider
        if fail_times > 0:
            self.provider = FlakyProvider(inner, fail_times)
        else:
            self.provider = inner
        self.stats = AgentStats()
        self._window_start = clock.now_ms()
        self._window_count = 0
        self._consecutive_failures = 0
        self._open_until = 0

    def plan(
        self,
        event: ChangeEvent,
        customer_state: dict[str, object] | None,
    ) -> OutboxPlan | None:
        """Pure rule match. `suppressed` is recorded when the kill switch is off."""
        if event.source.table != "shipments" or event.op == "d" or event.after is None:
            return None
        rule = _match(event, customer_state, self.slip_days)
        if rule is None:
            return None
        shipment_id = str(event.after.get("id", ""))
        status = str(event.after.get("status", ""))
        payload = {
            "shipment_id": shipment_id,
            "status": status,
            "rule": rule,
            "days": str(_slip_days(event) if rule == "date_slip" else 0),
        }
        return OutboxPlan(
            rule=rule,
            status="pending" if self.enabled else "suppressed",
            ts_ms=event.source.ts_ms,
            payload_json=json.dumps(payload, sort_keys=True),
        )

    def dispatch(self, store: Store, consumer: str) -> int:
        """Invoke the stub for pending rows, within the budget and the breaker."""
        pending = [
            row for row in store.list_outbox(consumer) if row.status == "pending"
        ]
        self.stats.backlog_peak = max(self.stats.backlog_peak, len(pending))
        if not pending or not self.enabled:
            return 0
        invoked = 0
        for cluster in _clusters(pending, self.coalesce_window_ms):
            if self._breaker_open():
                break
            if not self._budget_available():
                break
            winner = cluster[-1]
            try:
                draft = self._invoke(winner)
            except AgentFailure:
                self._on_failure()
                break
            with store.transaction():
                store.insert_draft(draft)
                store.update_outbox(consumer, winner.event_id, "invoked", None)
                for other in cluster[:-1]:
                    store.update_outbox(
                        consumer, other.event_id, "coalesced", winner.event_id
                    )
            self._on_success(winner.event_id)
            invoked += 1
        remaining = [
            row for row in store.list_outbox(consumer) if row.status == "pending"
        ]
        self.stats.backlog_peak = max(self.stats.backlog_peak, len(remaining))
        return invoked

    def backlog(self, store: Store, consumer: str) -> int:
        return sum(1 for row in store.list_outbox(consumer) if row.status == "pending")

    def approve(self, store: Store, draft_id: str, approver: str) -> None:
        if not approver:
            raise NotApproved("approver is required")
        with store.transaction():
            store.set_draft_status(draft_id, "approved", approver)

    def send(self, store: Store, draft_id: str) -> None:
        draft = store.get_draft(draft_id)
        if draft is None or draft.status != "approved" or not draft.approver:
            raise NotApproved(draft_id)
        with store.transaction():
            store.set_draft_status(draft_id, "sent", draft.approver)

    def _invoke(self, row: OutboxRow) -> DraftRow:
        response = self.provider.complete(
            CompletionRequest(
                model=_MODEL, system=_SYSTEM, prompt=row.rule, temperature=0.0
            )
        )
        payload = json.loads(row.payload_json)
        if not isinstance(payload, dict):
            raise AgentFailure("outbox payload is not an object")
        fields = {str(key): str(value) for key, value in payload.items()}
        body = fill_template(response.text, fields)
        return DraftRow(
            draft_id=f"draft-{row.event_id}",
            event_id=row.event_id,
            entity_key=row.entity_key,
            body=body,
            status="pending",
            approver=None,
        )

    def _budget_available(self) -> bool:
        now = self.clock.now_ms()
        if now - self._window_start >= self.window_ms:
            self._window_start = now
            self._window_count = 0
        return self._window_count < self.budget

    def _on_success(self, event_id: str) -> None:
        if self.stats.opened and self._open_until != 0:
            self.stats.closed_again = True
        self._consecutive_failures = 0
        self._open_until = 0
        self._window_count += 1
        self.stats.peak_in_window = max(self.stats.peak_in_window, self._window_count)
        self.stats.event_ids.append(event_id)

    def _on_failure(self) -> None:
        self.stats.failures += 1
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.breaker_threshold:
            self.stats.opened = True
            self._open_until = self.clock.now_ms() + self.cooldown_ms
            self._consecutive_failures = 0

    def _breaker_open(self) -> bool:
        if not self.stats.opened or self._open_until == 0:
            return False
        return self.clock.now_ms() < self._open_until


def _match(
    event: ChangeEvent, customer_state: dict[str, object] | None, slip_limit: int
) -> str | None:
    before = event.before or {}
    after = event.after or {}
    became_delayed = (
        after.get("status") == "delayed" and before.get("status") != "delayed"
    )
    tier = _tier(customer_state)
    if became_delayed and tier == "premium":
        return "premium_delayed"
    slipped = _slip_days(event)
    if slipped > slip_limit:
        return "date_slip"
    return None


def _tier(customer_state: dict[str, object] | None) -> str | None:
    if customer_state is None:
        return None
    after = customer_state.get("after")
    if isinstance(after, dict):
        tier = after.get("tier")
        if isinstance(tier, str):
            return tier
    return None


def _slip_days(event: ChangeEvent) -> int:
    if event.before is None or event.after is None:
        return 0
    before = event.before.get("promised_date")
    after = event.after.get("promised_date")
    if not isinstance(before, str) or not isinstance(after, str):
        return 0
    try:
        return (date.fromisoformat(after) - date.fromisoformat(before)).days
    except ValueError:
        return 0


def _clusters(rows: list[OutboxRow], window_ms: int) -> list[list[OutboxRow]]:
    grouped: dict[str, list[OutboxRow]] = {}
    for row in rows:
        grouped.setdefault(row.entity_key, []).append(row)
    clusters: list[list[OutboxRow]] = []
    for key in sorted(grouped):
        ordered = sorted(grouped[key], key=lambda row: (row.ts_ms, row.event_id))
        index = 0
        while index < len(ordered):
            cluster = [ordered[index]]
            cursor = index + 1
            while (
                cursor < len(ordered)
                and ordered[cursor].ts_ms - ordered[index].ts_ms <= window_ms
            ):
                cluster.append(ordered[cursor])
                cursor += 1
            clusters.append(cluster)
            index = cursor
    clusters.sort(key=lambda cluster: (cluster[-1].ts_ms, cluster[-1].event_id))
    return clusters
