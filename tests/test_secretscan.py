"""Tests for the vendored secretscan module (identical across portfolio repos).

Positive fixtures are assembled from pieces at run time, so this file never
contains a literal credential.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_lab import secretscan

A20 = "a1B2c3D4e5F6g7H8i9J0"  # 20 alphanumerics, obviously fake
H36 = "0123456789abcdefghijklmnopqrstuvwxyz"  # 36 chars

POSITIVE: dict[str, str] = {
    "xai_key": "xai" + "-" + A20 + "_-xyz",
    "anthropic_key": "sk" + "-ant-" + A20,
    "openai_key": "sk" + "-" + A20,
    "elevenlabs_key": "sk" + "_" + A20,
    "github_token": "ghp" + "_" + H36,
    "github_pat": "github" + "_pat_" + A20,
    "aws_access_key": "AK" + "IA" + "ABCDEFGHIJKLMNOP",
    "slack_token": "xox" + "b-" + "1234567890-abcdef",
    "bearer_token": "Bear" + "er " + A20 + "xyz",
    "private_key": "-----BEGIN " + "RSA " + "PRIVATE KEY-----",
}

NEGATIVE = [
    "the word token or secret in prose",
    "xai-short",
    "sk-short",
    "sk_short",
    "Bearer abc",
    "ghp_tooshort",
    "AKIA123",
    "task-queue-name-with-dashes-and-no-prefix-match-here",
    "-----BEGIN CERTIFICATE-----",
    "skeleton_key_is_not_a_key_01234567890123456789",
]


@pytest.mark.parametrize(("name", "text"), sorted(POSITIVE.items()))
def test_positive_fixtures_are_detected(name: str, text: str) -> None:
    assert name in secretscan.scan_text(f"value = {text}\n")


@pytest.mark.parametrize("text", NEGATIVE)
def test_negative_cases_are_clean(text: str) -> None:
    assert secretscan.scan_text(text) == []


def test_every_pattern_has_a_positive_fixture() -> None:
    assert {name for name, _ in secretscan.PATTERNS} == set(POSITIVE)


def test_scan_repo_reports_path_and_pattern(tmp_path: Path) -> None:
    (tmp_path / "leak.txt").write_text(POSITIVE["xai_key"], encoding="utf-8")
    (tmp_path / "ok.txt").write_text("nothing here", encoding="utf-8")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "skipped.txt").write_text(
        POSITIVE["xai_key"], encoding="utf-8"
    )
    assert secretscan.scan_repo(tmp_path) == ["leak.txt:xai_key"]


def test_main_exit_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ok.txt").write_text("clean", encoding="utf-8")
    assert secretscan.main() == 0
    (tmp_path / "bad.txt").write_text(POSITIVE["aws_access_key"], encoding="utf-8")
    assert secretscan.main() == 1


def test_vendored_file_hash_matches_provenance() -> None:
    """docs/PROVENANCE.md records `secretscan.py sha256: <hex>`; they must agree."""
    import hashlib
    import re

    root = Path(__file__).resolve().parents[1]
    provenance = (root / "docs" / "PROVENANCE.md").read_text(encoding="utf-8")
    match = re.search(r"secretscan\.py[^\n]*sha256[^0-9a-f]*([0-9a-f]{64})", provenance)
    assert match, "docs/PROVENANCE.md must record the sha256 of secretscan.py"
    digest = hashlib.sha256(Path(secretscan.__file__).read_bytes()).hexdigest()
    assert digest == match.group(1)
