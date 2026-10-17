"""Lag, counters, and a processing-latency histogram.

Latencies recorded from the simulated clock are labelled simulated wherever they
are printed. This module does not serve HTTP.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from cdc_lab.clock import Clock

BUCKETS_MS: tuple[int, ...] = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000)


@dataclass
class LagAlert:
    at_ms: int
    lag: int


@dataclass
class Metrics:
    clock: Clock
    lag_threshold: int
    lag_hold_ms: int
    retries: int = 0
    latencies_ms: list[int] = field(default_factory=list)
    lag_samples: list[tuple[int, int]] = field(default_factory=list)
    alerts: list[LagAlert] = field(default_factory=list)
    high_since_ms: int | None = None
    per_partition_lag: dict[int, int] = field(default_factory=dict)

    def observe_lag(self, per_partition: dict[int, int]) -> None:
        total = sum(per_partition.values())
        now = self.clock.now_ms()
        self.per_partition_lag = dict(per_partition)
        self.lag_samples.append((now, total))
        if total > self.lag_threshold:
            if self.high_since_ms is None:
                self.high_since_ms = now
            elif now - self.high_since_ms >= self.lag_hold_ms and not self.alerts:
                self.alerts.append(LagAlert(at_ms=now, lag=total))
        else:
            self.high_since_ms = None

    @property
    def lag_final(self) -> int:
        if not self.lag_samples:
            return 0
        return self.lag_samples[-1][1]

    @property
    def lag_max(self) -> int:
        if not self.lag_samples:
            return 0
        return max(sample for _ts, sample in self.lag_samples)

    @property
    def alerted(self) -> bool:
        return bool(self.alerts)


def percentile(values: list[int], pct: float) -> int | None:
    """Nearest-rank percentile. Empty input is None, not a fake zero."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, round(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def render_prometheus(snapshot: Snapshot) -> str:
    """Prometheus text exposition, stable key order, no timestamps."""
    lines: list[str] = []

    def counter(name: str, help_text: str, value: int) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {value}")

    lines.append(
        "# HELP cdc_consumer_lag_records Log end offset minus committed offset."
    )
    lines.append("# TYPE cdc_consumer_lag_records gauge")
    for partition in sorted(snapshot.per_partition_lag):
        lines.append(
            "cdc_consumer_lag_records"
            f'{{partition="{partition}"}} {snapshot.per_partition_lag[partition]}'
        )
    lines.append("# HELP cdc_consumer_lag_records_total Sum of per-partition lag.")
    lines.append("# TYPE cdc_consumer_lag_records_total gauge")
    lines.append(f"cdc_consumer_lag_records_total {snapshot.lag_total}")
    counter("cdc_events_applied_total", "Effects applied.", snapshot.applied)
    counter(
        "cdc_events_skipped_stale_total", "Older LSNs ignored.", snapshot.skipped_stale
    )
    counter(
        "cdc_events_duplicates_skipped_total",
        "Duplicate event ids skipped.",
        snapshot.duplicates_skipped,
    )
    counter("cdc_dlq_messages", "Records currently accounted as DLQ.", snapshot.dlq)
    counter("cdc_retries_total", "Transient handler failures.", snapshot.retries)
    counter(
        "cdc_agent_invocations_total",
        "Stub-agent drafts created.",
        snapshot.agent_invocations,
    )
    lines.append("# HELP cdc_processing_latency_ms Simulated processing latency.")
    lines.append("# TYPE cdc_processing_latency_ms histogram")
    for le, count in _histogram(snapshot.latencies_ms):
        lines.append(f'cdc_processing_latency_ms_bucket{{le="{le}"}} {count}')
    lines.append(f"cdc_processing_latency_ms_sum {sum(snapshot.latencies_ms)}")
    lines.append(f"cdc_processing_latency_ms_count {len(snapshot.latencies_ms)}")
    lines.append("# HELP cdc_lag_alert_firing Lag above threshold for the hold time.")
    lines.append("# TYPE cdc_lag_alert_firing gauge")
    lines.append(f"cdc_lag_alert_firing {1 if snapshot.alerted else 0}")
    return "\n".join(lines) + "\n"


def _histogram(values: list[int]) -> list[tuple[str, int]]:
    counts = [(str(le), sum(1 for value in values if value <= le)) for le in BUCKETS_MS]
    counts.append(("+Inf", len(values)))
    return counts


@dataclass(frozen=True)
class Snapshot:
    per_partition_lag: dict[int, int]
    lag_total: int
    applied: int
    skipped_stale: int
    duplicates_skipped: int
    dlq: int
    retries: int
    agent_invocations: int
    latencies_ms: list[int]
    alerted: bool
    lag_max: int
    latency_p50_ms: int | None
    latency_p95_ms: int | None

    def to_json(self) -> str:
        payload = {
            "clock": "simulated",
            "lag_total": self.lag_total,
            "lag_max": self.lag_max,
            "per_partition_lag": {
                str(k): v for k, v in sorted(self.per_partition_lag.items())
            },
            "applied": self.applied,
            "skipped_stale": self.skipped_stale,
            "duplicates_skipped": self.duplicates_skipped,
            "dlq": self.dlq,
            "retries": self.retries,
            "agent_invocations": self.agent_invocations,
            "alerted": self.alerted,
            "latency_ms": {
                "clock": "simulated",
                "p50": self.latency_p50_ms,
                "p95": self.latency_p95_ms,
                "count": len(self.latencies_ms),
            },
        }
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"
