"""Environment-only settings. There is no dotenv loader in this package."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from cdc_lab.errors import ConfigError


def _as_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _as_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be a boolean")


@dataclass(frozen=True)
class Settings:
    partitions: int = 4
    max_attempts: int = 5
    backoff_base_ms: int = 100
    backoff_cap_ms: int = 5_000
    visibility_ms: int = 30_000
    max_payload_bytes: int = 8_192
    lag_threshold: int = 1_000
    lag_hold_ms: int = 30_000
    agent_enabled: bool = True
    agent_budget: int = 50
    agent_window_ms: int = 60_000
    agent_breaker_threshold: int = 3
    agent_cooldown_ms: int = 30_000
    coalesce_window_ms: int = 5_000
    date_slip_days: int = 2
    sqlite_path: str = "var/cdc.sqlite"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        source = os.environ if env is None else env
        settings = cls(
            partitions=_as_int(source, "CDC_PARTITIONS", 4),
            max_attempts=_as_int(source, "CDC_MAX_ATTEMPTS", 5),
            backoff_base_ms=_as_int(source, "CDC_BACKOFF_BASE_MS", 100),
            backoff_cap_ms=_as_int(source, "CDC_BACKOFF_CAP_MS", 5_000),
            visibility_ms=_as_int(source, "CDC_VISIBILITY_MS", 30_000),
            max_payload_bytes=_as_int(source, "CDC_MAX_PAYLOAD_BYTES", 8_192),
            lag_threshold=_as_int(source, "CDC_LAG_THRESHOLD", 1_000),
            lag_hold_ms=_as_int(source, "CDC_LAG_HOLD_MS", 30_000),
            agent_enabled=_as_bool(source, "CDC_AGENT_ENABLED", True),
            agent_budget=_as_int(source, "CDC_AGENT_BUDGET", 50),
            agent_window_ms=_as_int(source, "CDC_AGENT_WINDOW_MS", 60_000),
            agent_breaker_threshold=_as_int(source, "CDC_AGENT_BREAKER_THRESHOLD", 3),
            agent_cooldown_ms=_as_int(source, "CDC_AGENT_COOLDOWN_MS", 30_000),
            coalesce_window_ms=_as_int(source, "CDC_COALESCE_WINDOW_MS", 5_000),
            date_slip_days=_as_int(source, "CDC_DATE_SLIP_DAYS", 2),
            sqlite_path=source.get("CDC_SQLITE_PATH", "var/cdc.sqlite")
            or "var/cdc.sqlite",
            log_level=source.get("CDC_LOG_LEVEL", "INFO") or "INFO",
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.partitions <= 0:
            raise ConfigError("CDC_PARTITIONS must be positive")
        if self.max_attempts <= 0:
            raise ConfigError("CDC_MAX_ATTEMPTS must be positive")
        if self.backoff_base_ms < 0 or self.backoff_cap_ms < 0:
            raise ConfigError("backoff must be non-negative")
        if self.backoff_cap_ms < self.backoff_base_ms:
            raise ConfigError("CDC_BACKOFF_CAP_MS must be >= CDC_BACKOFF_BASE_MS")
        if self.visibility_ms < 0 or self.max_payload_bytes <= 0:
            raise ConfigError("visibility and payload cap are invalid")
        if self.agent_budget < 0 or self.agent_window_ms <= 0:
            raise ConfigError("agent budget window is invalid")
        if self.agent_breaker_threshold <= 0 or self.agent_cooldown_ms < 0:
            raise ConfigError("agent breaker settings are invalid")
        if self.coalesce_window_ms < 0 or self.date_slip_days < 0:
            raise ConfigError("coalesce window and date slip must be non-negative")
        if self.lag_threshold < 0 or self.lag_hold_ms < 0:
            raise ConfigError("lag alert settings must be non-negative")
