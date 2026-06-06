"""Retry policy and helper for opus segment execution."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, TypeVar

from openneuronic.pipes.core.enums import GraphFailureMode

T = TypeVar("T")


@dataclass
class RetryPolicy:
    """Configures retry behaviour for a :class:`~openneuronic.pipes.opus.segment.PipeSegment`.

    Attributes:
        max_retries: Maximum number of retry attempts after the first failure.
        backoff_s: Base wait time in seconds between retries.
        jitter: When ``True``, add uniform random jitter of up to *backoff_s*
            to each wait to avoid thundering-herd effects.
        failure_mode: How the parent :class:`~openneuronic.pipes.opus.opus.Opus`
            should respond if retries are exhausted.
    """

    max_retries: int = 3
    backoff_s: float = 1.0
    jitter: bool = True
    failure_mode: GraphFailureMode = GraphFailureMode.RETRY_SEGMENT


async def run_with_retry(
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    policy: RetryPolicy,
) -> T:
    """Execute *coro_factory()* up to ``policy.max_retries + 1`` times.

    On each failure the coroutine is re-created from *coro_factory* and
    retried after an exponential back-off with optional jitter.

    Args:
        coro_factory: Zero-argument callable that returns a fresh coroutine
            each time it is called.  A fresh coroutine is required because
            coroutines cannot be restarted.
        policy: The :class:`RetryPolicy` controlling retry behaviour.

    Returns:
        The value returned by the coroutine on success.

    Raises:
        The last exception if all attempts are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(policy.max_retries + 1):
        try:
            return await coro_factory()
        except Exception as exc:
            last_exc = exc
            if attempt < policy.max_retries:
                wait = policy.backoff_s * (2 ** attempt)
                if policy.jitter:
                    wait += random.uniform(0, policy.backoff_s)
                await asyncio.sleep(wait)
    raise last_exc  # type: ignore[misc]
