from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from openneuronic.pipes.core.enums import CopyMode

if TYPE_CHECKING:
    from openneuronic.pipes.core.partial_scope import PartialScope
    from openneuronic.pipes.processors.base import Processor
    from openneuronic.pipes.sinks._base import AbstractSink
    from openneuronic.pipes.sources._base import AbstractSource


@dataclass
class Pipe:
    """The smallest runnable data movement unit.

    A ``Pipe`` wires together one source, zero or more processors, one sink,
    and a copy strategy.  Execution is handled externally by a runner (e.g.
    ``LocalRunner`` for in-process development or a distributed worker for
    production).
    """

    id: str
    source: AbstractSource
    sink: AbstractSink
    mode: CopyMode = CopyMode.INCREMENTAL
    schema: Any = None
    contract: Any = None
    processors: list[Processor] = field(default_factory=list)
    guards: list[Any] = field(default_factory=list)
    measures: list[Any] = field(default_factory=list)
    scope: PartialScope | None = None
