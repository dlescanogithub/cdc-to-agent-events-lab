"""CLI paths, including the forced regression exit code."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from cdc_lab.cli import main


def test_generate_run_demo_and_bench(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "events.jsonl"
    assert main(["generate", "--seed", "7", "--events", "12", "--out", str(out)]) == 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 12
    assert main(["generate", "--events", "2"]) == 0
    printed = capsys.readouterr().out
    assert "payload_b64" in printed
    work = tmp_path / "run"
    assert main(["run", "--seed", "7", "--events", "40", "--workdir", str(work)]) == 0
    report = json.loads((work / "report.json").read_text(encoding="utf-8"))
    assert report["clock"] == "simulated"
    assert (work / "metrics.prom").read_text(encoding="utf-8").startswith("# HELP")
    assert (work / "cdc.sqlite").is_file()
    assert main(["demo"]) == 0
    assert "clock: simulated" in capsys.readouterr().out
    assert main(["bench", "--events", "30", "--seed", "7"]) == 0
    bench = json.loads(capsys.readouterr().out)
    assert bench["label"] == "measured, in-memory, not a Kafka benchmark"
    assert bench["sample"] == 1
    assert bench["events"] == 30
    assert bench["violations"] == 0


def test_replay_flags_and_both_modes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    work = tmp_path / "saved"
    assert main(["run", "--events", "40", "--workdir", str(work)]) == 0
    capsys.readouterr()
    assert main(["replay", "--workdir", str(work)]) == 2
    assert "exactly one" in capsys.readouterr().err
    assert main(["replay", "--workdir", str(work), "--from-dlq"]) == 2
    assert "fix-profile" in capsys.readouterr().err
    assert (
        main(
            [
                "replay",
                "--workdir",
                str(work),
                "--from-dlq",
                "--fix-profile",
                "identity",
            ]
        )
        == 0
    )
    assert "replayable:" in capsys.readouterr().out
    assert (
        main(
            [
                "replay",
                "--workdir",
                str(work),
                "--from-offset",
                "0",
                "--partition",
                "0",
            ]
        )
        == 0
    )
    counts = json.loads(capsys.readouterr().out)
    assert isinstance(counts, dict)


def test_eval_commands_and_module_entrypoint() -> None:
    gate = subprocess.run(
        [sys.executable, "-m", "cdc_lab", "eval"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert gate.returncode == 0
    assert "violations: 0" in gate.stdout
    regression = subprocess.run(
        [sys.executable, "-m", "cdc_lab", "eval", "--regression"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert regression.returncode == 1
    assert "exactly_once_effect: pass=0 fail=1" in regression.stdout
    version = subprocess.run(
        [sys.executable, "-m", "cdc_lab", "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert version.returncode == 0
    assert "0.1.0" in version.stdout
