from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunMetrics:
    """Immutable snapshot of metrics collected during a single pipe run."""

    pipe_id: str
    records_read: int = 0
    records_written: int = 0
    records_dropped: int = 0
    batches: int = 0
    errors: int = 0
    duration_ms: float = 0.0
    throughput_rps: float = 0.0
    # Quality measures — populated only when the relevant measure is active.
    null_rates: dict[str, float] = field(default_factory=dict)
    duplicate_rate: float | None = None
    row_count_deviation: float | None = None


class Measure(ABC):
    """Base class for all opt-in telemetry measures.

    A measure is a named policy that collects specific metrics when it is
    attached to a :class:`~openneuronic.pipes.core.pipe.Pipe`.  Measures
    receive a :class:`~openneuronic.pipes.metrics.context.MetricsContext`
    and return a ``dict`` that is merged into the final :class:`RunMetrics`.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def collect(self, ctx: Any) -> dict[str, Any]:
        """Return a dict of metric name → value from the current context."""


class MeasureSet:
    """A named collection of :class:`Measure` instances."""

    def __init__(self, name: str, measures: list[Measure] | None = None) -> None:
        self.name = name
        self._measures: list[Measure] = measures or []

    def add(self, measure: Measure) -> MeasureSet:
        self._measures.append(measure)
        return self

    def collect_all(self, ctx: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for m in self._measures:
            result.update(m.collect(ctx))
        return result


class MeasureSetRegistry:
    def __init__(self) -> None:
        self._store: dict[str, MeasureSet] = {}

    def register(self, measure_set: MeasureSet) -> None:
        self._store[measure_set.name] = measure_set

    def get(self, name: str) -> MeasureSet:
        try:
            return self._store[name]
        except KeyError:
            raise KeyError(f"MeasureSet {name!r} is not registered") from None


measure_set_registry = MeasureSetRegistry()


class MeasureSetRef:
    """Lazy reference to a named :class:`MeasureSet` in the registry.

    Attach to a :class:`~openneuronic.pipes.core.pipe.Pipe` by name; resolved
    at runtime against :data:`measure_set_registry`.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def resolve(self) -> MeasureSet:
        return measure_set_registry.get(self.name)
