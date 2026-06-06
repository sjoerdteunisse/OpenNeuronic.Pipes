from __future__ import annotations

from typing import Any

from openneuronic.pipes.metrics.base import Measure


class RuntimeMeasure(Measure):
    """Collects core runtime metrics from a :class:`~openneuronic.pipes.metrics.context.MetricsContext`.

    Produces:
    - ``records_read``
    - ``records_written``
    - ``records_dropped``
    - ``batches``
    - ``errors``
    - ``duration_ms``
    - ``throughput_rps``
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        return {
            "records_read":    ctx.records_read,
            "records_written": ctx.records_written,
            "records_dropped": ctx.records_dropped,
            "batches":         ctx.batches,
            "errors":          ctx.errors,
            "duration_ms":     ctx.duration_ms,
            "throughput_rps":  ctx.throughput_rps,
        }
