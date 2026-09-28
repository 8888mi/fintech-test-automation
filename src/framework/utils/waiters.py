"""Polling helpers.

`time.sleep` in a test is a bug in waiting. It is either too short (flaky)
or too long (slow), and it is always both on someone else's machine.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def wait_until(
    condition: Callable[[], T | None],
    *,
    timeout: float = 10.0,
    interval: float = 0.25,
    message: str = "condition was not met",
) -> T:
    """Poll `condition` until it returns something truthy, or raise."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            result = condition()
            if result:
                return result
        except Exception as exc:
            last_error = exc
        time.sleep(interval)

    detail = f" Last error: {last_error!r}" if last_error else ""
    raise AssertionError(f"Timed out after {timeout:.1f}s: {message}.{detail}")
