from __future__ import annotations

import asyncio
import time

from openneuronic.pipes.guards.base import Guard, GuardContext, GuardResult


class RetryGuard(Guard):
    """Tracks consecutive failures and signals retry eligibility.

    The guard itself does not perform the retry — the runner consults the
    result to decide whether to retry the current batch.

    Args:
        max_retries: Number of consecutive failures allowed before the result
            is marked as a non-retriable failure.
    """

    def __init__(self, max_retries: int = 3) -> None:
        self._max_retries = max_retries
        self._consecutive_failures = 0

    async def check(self, ctx: GuardContext) -> GuardResult:
        # A RetryGuard is checked *after* a failure has already been recorded
        # by the runner.  Each call increments the counter; the guard passes
        # (= retry allowed) until the ceiling is hit.
        self._consecutive_failures += 1
        if self._consecutive_failures <= self._max_retries:
            return GuardResult(
                guard_name=self.name,
                passed=True,
                message=(
                    f"Retry {self._consecutive_failures}/{self._max_retries} allowed"
                ),
            )
        return GuardResult(
            guard_name=self.name,
            passed=False,
            message=(
                f"Max retries ({self._max_retries}) exceeded after "
                f"{self._consecutive_failures} consecutive failures"
            ),
        )

    def reset(self) -> None:
        """Call after a successful operation to reset the failure counter."""
        self._consecutive_failures = 0


class TimeoutGuard(Guard):
    """Fails if the operation recorded in context metadata exceeds a threshold.

    The runner is expected to populate ``ctx.metadata["duration_s"]`` before
    calling this guard.
    """

    def __init__(self, timeout_s: float) -> None:
        self._timeout_s = timeout_s

    async def check(self, ctx: GuardContext) -> GuardResult:
        duration = ctx.metadata.get("duration_s")
        if duration is None:
            return GuardResult(
                guard_name=self.name,
                passed=True,
                message="No duration recorded — timeout check skipped",
            )
        if duration > self._timeout_s:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Operation took {duration:.2f}s, "
                    f"exceeding timeout of {self._timeout_s:.2f}s"
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)


class CircuitBreakerGuard(Guard):
    """Implements a simple circuit-breaker pattern.

    States:
    - CLOSED (normal): requests pass through.
    - OPEN (tripped): requests are blocked until the recovery window elapses.
    - HALF-OPEN: next request is allowed as a probe; success closes the circuit.

    Args:
        failure_threshold: Consecutive failures before tripping.
        recovery_s: Seconds to wait before attempting recovery.
    """

    _CLOSED    = "closed"
    _OPEN      = "open"
    _HALF_OPEN = "half_open"

    def __init__(self, failure_threshold: int = 5, recovery_s: float = 30.0) -> None:
        self._threshold = failure_threshold
        self._recovery_s = recovery_s
        self._failures = 0
        self._state = self._CLOSED
        self._opened_at: float | None = None

    async def check(self, ctx: GuardContext) -> GuardResult:
        now = time.monotonic()

        if self._state == self._OPEN:
            if self._opened_at is not None and now - self._opened_at >= self._recovery_s:
                self._state = self._HALF_OPEN
            else:
                remaining = (
                    self._recovery_s - (now - (self._opened_at or now))
                )
                return GuardResult(
                    guard_name=self.name,
                    passed=False,
                    message=(
                        f"Circuit OPEN — recovery in {remaining:.1f}s"
                    ),
                )

        # Check whether the current batch signals a failure.
        failed = ctx.metadata.get("batch_failed", False)
        if failed:
            self._failures += 1
            if self._failures >= self._threshold:
                self._state = self._OPEN
                self._opened_at = now
                return GuardResult(
                    guard_name=self.name,
                    passed=False,
                    message=(
                        f"Circuit OPEN after {self._failures} consecutive failures"
                    ),
                )
            return GuardResult(
                guard_name=self.name,
                passed=True,
                message=f"Failure {self._failures}/{self._threshold} — circuit still CLOSED",
            )

        # Success — close the circuit.
        self._failures = 0
        self._state = self._CLOSED
        self._opened_at = None
        return GuardResult(guard_name=self.name, passed=True)

    @property
    def is_open(self) -> bool:
        return self._state == self._OPEN
