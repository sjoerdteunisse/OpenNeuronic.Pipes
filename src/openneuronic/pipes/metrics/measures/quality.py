from __future__ import annotations

from typing import Any

from openneuronic.pipes.metrics.base import Measure


class NullRateMeasure(Measure):
    """Reports null rates per field collected by :class:`MetricsContext`.

    Produces: ``null_rates`` (dict mapping field name → fraction).
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        return {"null_rates": ctx._null_counts and {
            f: ctx._null_counts[f] / ctx._total_for_null.get(f, 1)
            for f in ctx._null_counts
        } or {}}


class DuplicateRateMeasure(Measure):
    """Reports the duplicate record rate collected by :class:`MetricsContext`.

    Produces: ``duplicate_rate`` (float or None).
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        if ctx.records_read > 0 and ctx._duplicate_count > 0:
            return {"duplicate_rate": ctx._duplicate_count / ctx.records_read}
        return {"duplicate_rate": None}


class RowCountDeviationMeasure(Measure):
    """Reports how far actual row count deviated from an expected baseline.

    Produces: ``row_count_deviation`` (float or None).
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        expected = ctx._row_count_expected
        if expected and expected > 0:
            deviation = abs(ctx.records_written - expected) / expected
            return {"row_count_deviation": round(deviation, 4)}
        return {"row_count_deviation": None}
