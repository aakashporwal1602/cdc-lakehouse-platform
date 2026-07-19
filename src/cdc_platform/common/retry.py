"""Reusable retry policies built on ``tenacity``.

Centralising retry configuration means every external I/O call (HTTP to Connect,
Kafka admin, JDBC) uses the same battle-tested backoff behaviour instead of
ad-hoc loops.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

T = TypeVar("T")


def with_retries(
    *,
    attempts: int = 5,
    min_wait: float = 1.0,
    max_wait: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator applying exponential backoff with jitter to transient failures."""

    return retry(
        reraise=True,
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=min_wait, max=max_wait),
        retry=retry_if_exception_type(exceptions),
    )
