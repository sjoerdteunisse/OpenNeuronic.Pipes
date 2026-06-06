from __future__ import annotations

import datetime
from typing import Any

from openneuronic.pipes.guards.base import Guard, GuardContext, GuardResult


class NullSpikeGuard(Guard):
    """Fails when the null rate for a field exceeds *max_null_pct*.

    Args:
        field: Payload field name to check.
        max_null_pct: Maximum acceptable fraction of null values (0.0–1.0).
    """

    def __init__(self, field: str, max_null_pct: float = 0.05) -> None:
        self._field = field
        self._max = max_null_pct

    async def check(self, ctx: GuardContext) -> GuardResult:
        if not ctx.batch:
            return GuardResult(guard_name=self.name, passed=True)
        null_count = sum(
            1 for r in ctx.batch
            if r.payload.get(self._field) is None
        )
        rate = null_count / len(ctx.batch)
        if rate > self._max:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Null rate for field {self._field!r} is {rate:.1%}, "
                    f"exceeding threshold {self._max:.1%} "
                    f"({null_count}/{len(ctx.batch)} nulls)"
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)


class EmptyColumnGuard(Guard):
    """Fails when an entire batch has no non-null values for a field.

    Useful for catching columns that were accidentally excluded from queries.

    Args:
        field: Payload field name to check.
    """

    def __init__(self, field: str) -> None:
        self._field = field

    async def check(self, ctx: GuardContext) -> GuardResult:
        if not ctx.batch:
            return GuardResult(guard_name=self.name, passed=True)
        has_value = any(
            r.payload.get(self._field) is not None for r in ctx.batch
        )
        if not has_value:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Field {self._field!r} is entirely null/missing across "
                    f"all {len(ctx.batch)} records in this batch"
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)


class FreshnessGuard(Guard):
    """Fails when the maximum value of a datetime field is older than *max_age_seconds*.

    Args:
        field: Payload field name holding a :class:`datetime.datetime` value.
        max_age_seconds: How many seconds old the most recent record may be.
    """

    def __init__(self, field: str, max_age_seconds: float) -> None:
        self._field = field
        self._max_age = max_age_seconds

    async def check(self, ctx: GuardContext) -> GuardResult:
        values: list[Any] = [
            r.payload[self._field]
            for r in ctx.batch
            if self._field in r.payload and r.payload[self._field] is not None
        ]
        if not values:
            return GuardResult(
                guard_name=self.name,
                passed=True,
                message=f"No values found for field {self._field!r} — freshness check skipped",
            )
        try:
            latest: datetime.datetime = max(values)
        except TypeError:
            return GuardResult(
                guard_name=self.name,
                passed=True,
                message=f"Cannot compare values for field {self._field!r} — freshness check skipped",
            )

        if not isinstance(latest, datetime.datetime):
            return GuardResult(guard_name=self.name, passed=True)

        now = datetime.datetime.now(datetime.UTC)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=datetime.UTC)
        age = (now - latest).total_seconds()
        if age > self._max_age:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Field {self._field!r} is {age:.0f}s old, "
                    f"exceeding freshness threshold of {self._max_age:.0f}s"
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)
