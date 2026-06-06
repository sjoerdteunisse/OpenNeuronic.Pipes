"""Lineage event model — emitted once per meaningful data movement boundary."""
from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from enum import auto
from typing import Any

try:
    from enum import StrEnum
except ImportError:  # Python < 3.11
    from enum import Enum

    class StrEnum(str, Enum):  # type: ignore[no-redef]
        pass


from openneuronic.pipes.lineage.refs import ColumnRef, DatasetRef


class LineageEventKind(StrEnum):
    READ      = "read"
    WRITE     = "write"
    DERIVE    = "derive"
    TRANSFORM = "transform"


@dataclass
class LineageEvent:
    """One provenance event emitted during a pipe run.

    Attributes:
        kind: What happened (READ / WRITE / DERIVE / TRANSFORM).
        pipe_id: The pipe that produced this event.
        run_id: The run identifier (UUID string).
        source_ref: Dataset that was read, when applicable.
        target_ref: Dataset that was written, when applicable.
        schema_version: Schema version active at the time of the event.
        contract_version: Contract version active at the time of the event.
        emitted_at: UTC-aware timestamp.
        event_id: Unique event identifier (UUID).
        metadata: Arbitrary additional context.
    """

    kind: LineageEventKind
    pipe_id: str
    run_id: str
    source_ref: DatasetRef | None = None
    target_ref: DatasetRef | None = None
    schema_version: int = 1
    contract_version: int = 1
    emitted_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ColumnLineage:
    """Declares how one output column is derived from one or more inputs.

    Attributes:
        output: The produced :class:`~openneuronic.pipes.lineage.refs.ColumnRef`.
        inputs: Source columns the derivation reads from.
        expression: Human-readable expression, e.g. ``"amount * eur_rate"``.
        transform_kind: One of ``"copy"``, ``"derive"``, ``"aggregate"``, ``"cast"``.
    """

    output: ColumnRef
    inputs: list[ColumnRef] = field(default_factory=list)
    expression: str = ""
    transform_kind: str = "copy"


@dataclass
class DatasetLineage:
    """Summarises all column-level relationships between a source and a target.

    Attributes:
        source: The source :class:`~openneuronic.pipes.lineage.refs.DatasetRef`.
        target: The sink :class:`~openneuronic.pipes.lineage.refs.DatasetRef`.
        pipe_id: The pipe that moved the data.
        run_id: The run identifier.
        column_lineages: Zero or more :class:`ColumnLineage` declarations.
    """

    source: DatasetRef
    target: DatasetRef
    pipe_id: str
    run_id: str
    column_lineages: list[ColumnLineage] = field(default_factory=list)
