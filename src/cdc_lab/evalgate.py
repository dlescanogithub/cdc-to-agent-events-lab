"""Eval gate. Exit 0 when every invariant holds, non-zero otherwise.

`--regression` runs the duplicate burst with idempotency disabled. That component
is supposed to fail the exactly-once check. CI asserts the exit code is 1.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from cdc_lab.invariants import INVARIANTS, RunTrace, Violation, check, is_non_vacuous
from cdc_lab.pipeline import RunResult, run_scenario
from cdc_lab.scenarios import load_scenarios


def scenario_directory() -> Path:
    packaged = Path(__file__).resolve().parents[2] / "scenarios"
    if packaged.is_dir():
        return packaged
    raise FileNotFoundError("scenarios/ directory not found next to the source tree")


def run_eval(regression: bool, directory: Path | None = None) -> tuple[int, str]:
    scenarios = load_scenarios(directory or scenario_directory())
    mode = "regression" if regression else "gate"
    if regression:
        base = next(item for item in scenarios if item.name == "duplicate_burst")
        scenarios = [
            replace(
                base,
                name="regression_no_idempotency",
                idempotency=False,
                replay=False,
            )
        ]
    results = [run_scenario(scenario) for scenario in scenarios]
    lines = [f"mode: {mode}", f"scenarios: {len(results)}"]
    for name, _fn in INVARIANTS:
        passed = 0
        failed = 0
        non_vacuous = 0
        for result in results:
            found = [item for item in check(result.trace) if item.invariant == name]
            if found:
                failed += 1
            else:
                passed += 1
            if is_non_vacuous(name, result.trace):
                non_vacuous += 1
        lines.append(f"{name}: pass={passed} fail={failed} non_vacuous={non_vacuous}")
    violations = _all_violations(results)
    lines.append(f"violations: {len(violations)}")
    for violation in violations:
        lines.append(f"violation: {violation.invariant}: {violation.message}")
    if regression:
        lines.append("component: consumer idempotency disabled on duplicate_burst")
    lines.append("")
    return (1 if violations else 0), "\n".join(lines)


def _all_violations(results: list[RunResult]) -> list[Violation]:
    found: list[Violation] = []
    for result in results:
        found.extend(check(result.trace))
    return found


def assert_trace_shape(trace: RunTrace) -> None:
    """Imported by tests that want the trace type without running the gate."""
    if trace.produced < 0:
        raise ValueError("produced cannot be negative")
