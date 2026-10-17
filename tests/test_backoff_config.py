"""Backoff schedule properties and environment configuration."""

from __future__ import annotations

import random

import pytest

from cdc_lab.backoff import full_jitter_ms
from cdc_lab.config import Settings
from cdc_lab.errors import ConfigError
from cdc_lab.yamlutil import parse_flat_yaml


def test_full_jitter_stays_inside_the_cap_and_is_deterministic() -> None:
    rng = random.Random(7)
    values = [full_jitter_ms(rng, exponent, 100, 1_000) for exponent in range(8)]
    assert values[0] <= 100
    assert all(0 <= delay <= 1_000 for delay in values)
    again = random.Random(7)
    expected = [full_jitter_ms(again, exponent, 100, 1_000) for exponent in range(8)]
    assert values == expected
    assert len(set(values)) > 1
    assert full_jitter_ms(random.Random(1), 0, 0, 10) == 0
    with pytest.raises(ValueError):
        full_jitter_ms(random.Random(1), -1, 1, 1)


def test_settings_from_env_and_rejects_bad_values() -> None:
    settings = Settings.from_env({})
    assert settings.partitions == 4
    assert settings.agent_enabled is True
    overridden = Settings.from_env({"CDC_PARTITIONS": "2", "CDC_AGENT_ENABLED": "off"})
    assert overridden.partitions == 2
    assert overridden.agent_enabled is False
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_PARTITIONS": "nope"})
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_AGENT_ENABLED": "maybe"})
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_BACKOFF_BASE_MS": "50", "CDC_BACKOFF_CAP_MS": "10"})
    with pytest.raises(ConfigError):
        Settings.from_env({"CDC_MAX_ATTEMPTS": "0"})


def test_flat_yaml_scalars() -> None:
    parsed = parse_flat_yaml(
        'name: demo\ncount: 3\nenabled: true\nnote: null\n# comment\nlabel: "ab"\n'
    )
    assert parsed == {
        "name": "demo",
        "count": 3,
        "enabled": True,
        "note": None,
        "label": "ab",
    }
    with pytest.raises(ValueError, match="flat"):
        parse_flat_yaml("name: demo\n  child: 1\n")
    with pytest.raises(ValueError, match="duplicate"):
        parse_flat_yaml("name: a\nname: b\n")
