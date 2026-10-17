"""Full-jitter exponential backoff.

sleep = random(0, min(cap, base * 2**exponent)), inclusive of both ends.
"""

from __future__ import annotations

import random


def full_jitter_ms(rng: random.Random, exponent: int, base_ms: int, cap_ms: int) -> int:
    """Return a delay in milliseconds. Never exceeds `cap_ms`."""
    if exponent < 0 or base_ms < 0 or cap_ms < 0:
        raise ValueError("backoff parameters must be non-negative")
    ceiling = min(cap_ms, base_ms * (2**exponent))
    if ceiling <= 0:
        return 0
    return rng.randrange(0, ceiling + 1)
