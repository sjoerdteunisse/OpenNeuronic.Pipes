from __future__ import annotations

from typing import Any

from openneuronic.pipes.metrics.base import Measure


class WaitTimeMeasure(Measure):
    """Stub for M6 orchestration wait-time measurement.

    Will track time segments spend waiting for upstream dependencies in an Opus.
    Returns empty dict until M6 wires orchestration state.
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        return {}


class CriticalPathMeasure(Measure):
    """Stub for M6 critical-path duration measurement.

    Returns empty dict until M6 wires orchestration state.
    """

    def collect(self, ctx: Any) -> dict[str, Any]:
        return {}
