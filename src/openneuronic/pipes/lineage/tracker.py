"""LineageTracker — collects lineage events during a single pipe run."""
from __future__ import annotations

import uuid

from openneuronic.pipes.lineage.events import (
    DatasetLineage,
    LineageEvent,
    LineageEventKind,
)
from openneuronic.pipes.lineage.graph.model import KnowledgeGraph
from openneuronic.pipes.lineage.refs import DatasetRef


class LineageTracker:
    """Collects :class:`~openneuronic.pipes.lineage.events.LineageEvent` objects
    during a pipe run and can materialise them into a :class:`KnowledgeGraph`.

    One tracker per run is created by :class:`~openneuronic.pipes.runner.LocalRunner`
    and attached to the resulting :class:`~openneuronic.pipes.runner.RunResult`.
    """

    def __init__(self, run_id: str | None = None) -> None:
        self.run_id: str = run_id or str(uuid.uuid4())
        self._events: list[LineageEvent] = []

    # ------------------------------------------------------------------
    # Emit
    # ------------------------------------------------------------------

    def emit(self, event: LineageEvent) -> None:
        self._events.append(event)

    def track_read(
        self,
        pipe_id: str,
        source_ref: DatasetRef,
        schema_version: int = 1,
    ) -> None:
        """Record that *pipe_id* read from *source_ref*."""
        self.emit(
            LineageEvent(
                kind=LineageEventKind.READ,
                pipe_id=pipe_id,
                run_id=self.run_id,
                source_ref=source_ref,
                schema_version=schema_version,
            )
        )

    def track_write(
        self,
        pipe_id: str,
        target_ref: DatasetRef,
        schema_version: int = 1,
        contract_version: int = 1,
    ) -> None:
        """Record that *pipe_id* wrote to *target_ref*."""
        self.emit(
            LineageEvent(
                kind=LineageEventKind.WRITE,
                pipe_id=pipe_id,
                run_id=self.run_id,
                target_ref=target_ref,
                schema_version=schema_version,
                contract_version=contract_version,
            )
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    @property
    def events(self) -> list[LineageEvent]:
        return list(self._events)

    def events_for_run(self, run_id: str | None = None) -> list[LineageEvent]:
        rid = run_id or self.run_id
        return [e for e in self._events if e.run_id == rid]

    # ------------------------------------------------------------------
    # Materialise
    # ------------------------------------------------------------------

    def to_graph(self) -> KnowledgeGraph:
        """Build a :class:`KnowledgeGraph` from all collected events."""
        graph = KnowledgeGraph()
        for event in self._events:
            graph.add_event(event)
        return graph

    def to_dataset_lineage(
        self, pipe_id: str, source_ref: DatasetRef, target_ref: DatasetRef
    ) -> DatasetLineage:
        """Build a :class:`~openneuronic.pipes.lineage.events.DatasetLineage`
        summarising the relationship between source and target for this run."""
        return DatasetLineage(
            source=source_ref,
            target=target_ref,
            pipe_id=pipe_id,
            run_id=self.run_id,
        )
