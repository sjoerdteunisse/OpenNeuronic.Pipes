from __future__ import annotations

import functools
import time
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from openneuronic.pipes.metrics.context import MetricsContext

# ContextVar holding the active MetricsContext for the current async task.
_active_context: ContextVar[MetricsContext | None] = ContextVar(
    "_active_context", default=None
)


def get_active_context() -> MetricsContext | None:
    """Return the :class:`MetricsContext` active in the current async context, if any."""
    return _active_context.get()


def set_active_context(ctx: MetricsContext | None) -> Any:
    """Bind *ctx* as the active context and return the token for later reset."""
    return _active_context.set(ctx)


def measure(name: str | None = None) -> Callable:
    """Decorator that records the wall-clock duration of an async function into the
    active :class:`MetricsContext`.

    The duration is stored in ``ctx.metadata`` under *name* (defaults to the
    wrapped function's qualified name).  If no context is active the function
    runs unchanged.

    Usage::

        @measure("sink_write")
        async def write(self, records):
            ...
    """
    def decorator(fn: Callable) -> Callable:
        metric_name = name or fn.__qualname__

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            ctx = _active_context.get()
            if ctx is None:
                return await fn(*args, **kwargs)
            t0 = time.monotonic()
            try:
                return await fn(*args, **kwargs)
            finally:
                elapsed = time.monotonic() - t0
                # Stash per-operation timing in a side-channel dict on context.
                timings = getattr(ctx, "_op_timings", None)
                if timings is None:
                    ctx._op_timings = {}  # type: ignore[attr-defined]
                ctx._op_timings[metric_name] = elapsed  # type: ignore[attr-defined]

        return wrapper
    return decorator
