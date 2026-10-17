"""Load flat scenario files. Unknown keys are errors so a typo cannot be silent."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from cdc_lab.errors import ConfigError
from cdc_lab.yamlutil import parse_flat_yaml

_INT_FIELDS = (
    "seed",
    "events",
    "customers",
    "shipments",
    "duplicates",
    "ooo_swaps",
    "malformed",
    "schema_violations",
    "unknown_versions",
    "oversized",
    "partitions",
    "crash_times",
    "handler_fault_times",
    "agent_fail_times",
    "agent_budget",
    "agent_window_ms",
    "agent_cooldown_ms",
    "agent_breaker_threshold",
    "coalesce_window_ms",
    "slip_days",
    "service_ms",
    "lag_threshold",
    "lag_hold_ms",
    "max_attempts",
    "backoff_base_ms",
    "backoff_cap_ms",
    "visibility_ms",
    "batch_size",
    "max_bytes",
    "dispatch_rounds",
)
_BOOL_FIELDS = ("idempotency", "replay", "agent_enabled")
_OPT_STR_FIELDS = ("crash_point", "fix_profile")


@dataclass(frozen=True)
class Scenario:
    name: str
    builder: str
    seed: int = 7
    events: int = 40
    customers: int = 6
    shipments: int = 8
    duplicates: int = 0
    ooo_swaps: int = 0
    malformed: int = 0
    schema_violations: int = 0
    unknown_versions: int = 0
    oversized: int = 0
    partitions: int = 4
    idempotency: bool = True
    crash_point: str | None = None
    crash_times: int = 1
    handler_fault_times: int = 0
    replay: bool = False
    fix_profile: str | None = None
    agent_fail_times: int = 0
    agent_budget: int = 50
    agent_window_ms: int = 60_000
    agent_cooldown_ms: int = 5_000
    agent_breaker_threshold: int = 3
    coalesce_window_ms: int = 5_000
    slip_days: int = 2
    agent_enabled: bool = True
    service_ms: int = 0
    lag_threshold: int = 10_000
    lag_hold_ms: int = 30_000
    max_attempts: int = 5
    backoff_base_ms: int = 10
    backoff_cap_ms: int = 200
    visibility_ms: int = 1_000
    batch_size: int = 8
    max_bytes: int = 8192
    dispatch_rounds: int = 4


_ALLOWED = (
    set(_INT_FIELDS) | set(_BOOL_FIELDS) | set(_OPT_STR_FIELDS) | {"name", "builder"}
)
_DEFAULTS = Scenario(name="_", builder="_")


def scenario_from_mapping(data: Mapping[str, object]) -> Scenario:
    unknown = sorted(set(data) - _ALLOWED)
    if unknown:
        raise ConfigError(f"unknown scenario fields: {', '.join(unknown)}")
    if "name" not in data or "builder" not in data:
        raise ConfigError("scenario requires name and builder")
    name = data["name"]
    builder = data["builder"]
    if not isinstance(name, str) or not name:
        raise ConfigError("name must be a non-empty string")
    if not isinstance(builder, str) or not builder:
        raise ConfigError("builder must be a non-empty string")
    return replace(
        _DEFAULTS,
        name=name,
        builder=builder,
        seed=_as_int(data, "seed"),
        events=_as_int(data, "events"),
        customers=_as_int(data, "customers"),
        shipments=_as_int(data, "shipments"),
        duplicates=_as_int(data, "duplicates"),
        ooo_swaps=_as_int(data, "ooo_swaps"),
        malformed=_as_int(data, "malformed"),
        schema_violations=_as_int(data, "schema_violations"),
        unknown_versions=_as_int(data, "unknown_versions"),
        oversized=_as_int(data, "oversized"),
        partitions=_as_int(data, "partitions"),
        idempotency=_as_bool(data, "idempotency"),
        crash_point=_as_opt_str(data, "crash_point"),
        crash_times=_as_int(data, "crash_times"),
        handler_fault_times=_as_int(data, "handler_fault_times"),
        replay=_as_bool(data, "replay"),
        fix_profile=_as_opt_str(data, "fix_profile"),
        agent_fail_times=_as_int(data, "agent_fail_times"),
        agent_budget=_as_int(data, "agent_budget"),
        agent_window_ms=_as_int(data, "agent_window_ms"),
        agent_cooldown_ms=_as_int(data, "agent_cooldown_ms"),
        agent_breaker_threshold=_as_int(data, "agent_breaker_threshold"),
        coalesce_window_ms=_as_int(data, "coalesce_window_ms"),
        slip_days=_as_int(data, "slip_days"),
        agent_enabled=_as_bool(data, "agent_enabled"),
        service_ms=_as_int(data, "service_ms"),
        lag_threshold=_as_int(data, "lag_threshold"),
        lag_hold_ms=_as_int(data, "lag_hold_ms"),
        max_attempts=_as_int(data, "max_attempts"),
        backoff_base_ms=_as_int(data, "backoff_base_ms"),
        backoff_cap_ms=_as_int(data, "backoff_cap_ms"),
        visibility_ms=_as_int(data, "visibility_ms"),
        batch_size=_as_int(data, "batch_size"),
        max_bytes=_as_int(data, "max_bytes"),
        dispatch_rounds=_as_int(data, "dispatch_rounds"),
    )


def load_scenario(path: Path) -> Scenario:
    return scenario_from_mapping(parse_flat_yaml(path.read_text(encoding="utf-8")))


def load_scenarios(directory: Path) -> list[Scenario]:
    paths = sorted(directory.glob("*.yaml"))
    if not paths:
        raise ConfigError(f"no scenarios in {directory}")
    return [load_scenario(path) for path in paths]


def _present(data: Mapping[str, object], field: str) -> bool:
    return field in data and data[field] is not None


def _as_int(data: Mapping[str, object], field: str) -> int:
    if not _present(data, field):
        return int(getattr(_DEFAULTS, field))
    value = data[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{field} must be an integer")
    return value


def _as_bool(data: Mapping[str, object], field: str) -> bool:
    if not _present(data, field):
        return bool(getattr(_DEFAULTS, field))
    value = data[field]
    if not isinstance(value, bool):
        raise ConfigError(f"{field} must be a boolean")
    return value


def _as_opt_str(data: Mapping[str, object], field: str) -> str | None:
    if field not in data or data[field] is None:
        return None
    value = data[field]
    if not isinstance(value, str):
        raise ConfigError(f"{field} must be a string")
    return value
