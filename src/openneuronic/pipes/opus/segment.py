"""PipeSegment — a bounded unit of work inside an Opus."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from openneuronic.pipes.core.enums import WaitStrategy

if TYPE_CHECKING:
    from openneuronic.pipes.core.pipe import Pipe
    from openneuronic.pipes.opus.retry import RetryPolicy


@dataclass
class PipeSegment:
    """One bounded unit of work inside an :class:`~openneuronic.pipes.opus.opus.Opus`.

    Attributes:
        id: Unique identifier within the opus.  Used in :attr:`depends_on`
            references of downstream segments.
        pipe: The :class:`~openneuronic.pipes.core.pipe.Pipe` to execute.
        depends_on: IDs of segments that must complete before this one can
            start.  Empty means the segment is a root (no dependencies).
        wait_strategy: Controls how many upstream dependencies must be
            satisfied before this segment is eligible to run.
            Only :attr:`~openneuronic.pipes.core.enums.WaitStrategy.ALL`
            is fully implemented; other values are accepted and treated
            as ``ALL`` in this version.
        retry_policy: Optional per-segment retry configuration.  Falls back
            to the :class:`~openneuronic.pipes.opus.opus.Opus`-level policy
            when ``None``.
    """

    id: str
    pipe: Pipe
    depends_on: list[str] = field(default_factory=list)
    wait_strategy: WaitStrategy = WaitStrategy.ALL
    retry_policy: RetryPolicy | None = None
