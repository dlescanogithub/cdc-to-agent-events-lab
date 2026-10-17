"""Injectable clocks. Simulated time is never reported as a measurement."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int:
        """Monotonic milliseconds."""

    def advance(self, ms: int) -> None:
        """Move the clock forward. Simulated clocks jump; a wall clock sleeps."""


class SimulatedClock:
    """Deterministic clock. `advance` is the only way it moves."""

    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, ms: int) -> None:
        if ms < 0:
            raise ValueError("simulated clock cannot move backwards")
        self._now += ms


class WallClock:
    """Real wall clock. Used only by the optional in-memory micro-benchmark."""

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def advance(self, ms: int) -> None:
        if ms < 0:
            raise ValueError("clock cannot move backwards")
        if ms:
            time.sleep(ms / 1000)
