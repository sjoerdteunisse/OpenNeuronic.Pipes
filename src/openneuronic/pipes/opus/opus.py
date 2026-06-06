"""Opus — a composed, durable orchestration of one or more PipeSegments."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from openneuronic.pipes.core.enums import GraphFailureMode, WaitStrategy
from openneuronic.pipes.opus.segment import PipeSegment


class Opus:
    """Composed durable execution of one or more :class:`PipeSegment` instances.

    An :class:`Opus` validates that its segment graph is a DAG (no cycles) and
    exposes a :meth:`topological_waves` view used by the runner.

    Example::

        opus = Opus(id="nightly-orders", durable=True)
        opus.add_segments([
            PipeSegment(id="extract", pipe=extract_pipe),
            PipeSegment(id="transform", pipe=tx_pipe, depends_on=["extract"]),
            PipeSegment(id="load", pipe=load_pipe, depends_on=["transform"]),
        ])

    Args:
        id: Unique opus identifier.
        schedule: Optional cron expression for scheduled execution,
            e.g. ``"0 2 * * *"``.
        on_failure: How to handle segment failures across the graph.
        durable: When ``True`` segment states are persisted to the
            :class:`~openneuronic.pipes.opus.state.DurableRunState` store so
            runs can resume after a crash.
    """

    def __init__(
        self,
        id: str,
        schedule: str | None = None,
        on_failure: GraphFailureMode = GraphFailureMode.FAIL_FAST,
        durable: bool = False,
    ) -> None:
        self.id = id
        self.schedule = schedule
        self.on_failure = on_failure
        self.durable = durable
        self._segments: dict[str, PipeSegment] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_segment(self, segment: PipeSegment) -> None:
        """Add a single *segment* to this opus."""
        if segment.id in self._segments:
            raise ValueError(f"Segment id {segment.id!r} already registered in opus {self.id!r}")
        self._segments[segment.id] = segment
        self._validate_dag()

    def add_segments(self, segments: list[PipeSegment]) -> None:
        """Add multiple *segments* at once.  Validates the DAG after all are added."""
        for seg in segments:
            if seg.id in self._segments:
                raise ValueError(f"Segment id {seg.id!r} already registered in opus {self.id!r}")
            self._segments[seg.id] = seg
        self._validate_dag()

    # ------------------------------------------------------------------
    # DAG validation
    # ------------------------------------------------------------------

    def _validate_dag(self) -> None:
        """Raise :exc:`ValueError` if the dependency graph contains a cycle
        or references an unknown segment ID."""
        for seg in self._segments.values():
            for dep in seg.depends_on:
                if dep not in self._segments:
                    raise ValueError(
                        f"Segment {seg.id!r} depends_on unknown segment {dep!r}"
                    )
        # Kahn's algorithm to detect cycles
        in_degree: dict[str, int] = {sid: 0 for sid in self._segments}
        for seg in self._segments.values():
            for dep in seg.depends_on:
                in_degree[seg.id] += 1

        queue = [sid for sid, deg in in_degree.items() if deg == 0]
        visited = 0
        dependents: dict[str, list[str]] = defaultdict(list)
        for seg in self._segments.values():
            for dep in seg.depends_on:
                dependents[dep].append(seg.id)

        while queue:
            sid = queue.pop()
            visited += 1
            for dependent_id in dependents[sid]:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)

        if visited != len(self._segments):
            raise ValueError(f"Opus {self.id!r} contains a dependency cycle")

    # ------------------------------------------------------------------
    # Topological ordering
    # ------------------------------------------------------------------

    def topological_waves(self) -> list[list[PipeSegment]]:
        """Return segments grouped into parallel *waves*.

        Each wave contains all segments whose dependencies are satisfied by
        the completion of all previous waves.  Segments within a wave are
        independent and can run concurrently.

        Segments with :attr:`~PipeSegment.wait_strategy` of
        :attr:`~WaitStrategy.NONE` are placed in wave 0, even if they have
        ``depends_on`` entries (they do not wait for those dependencies).
        All other strategies are treated as ``ALL`` in this version.
        """
        # Segments that never wait are immediately placed in wave 0.
        immediate_ids = {
            seg.id
            for seg in self._segments.values()
            if seg.wait_strategy == WaitStrategy.NONE
        }
        waiting = {
            sid: seg
            for sid, seg in self._segments.items()
            if sid not in immediate_ids
        }

        in_degree: dict[str, int] = {}
        dependents: dict[str, list[str]] = defaultdict(list)
        for sid, seg in waiting.items():
            relevant_deps = [d for d in seg.depends_on if d in waiting]
            in_degree[sid] = len(relevant_deps)
            for dep in relevant_deps:
                dependents[dep].append(sid)

        waves: list[list[PipeSegment]] = []
        if immediate_ids:
            waves.append([self._segments[sid] for sid in immediate_ids])

        ready = [sid for sid, deg in in_degree.items() if deg == 0]
        while ready:
            waves.append([waiting[sid] for sid in ready])
            next_ready: list[str] = []
            for sid in ready:
                for dep_sid in dependents[sid]:
                    in_degree[dep_sid] -= 1
                    if in_degree[dep_sid] == 0:
                        next_ready.append(dep_sid)
            ready = next_ready

        return waves

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def segments(self) -> list[PipeSegment]:
        return list(self._segments.values())

    def get_segment(self, segment_id: str) -> PipeSegment | None:
        return self._segments.get(segment_id)

    def __repr__(self) -> str:
        return (
            f"Opus(id={self.id!r}, segments={len(self._segments)}, "
            f"durable={self.durable})"
        )
