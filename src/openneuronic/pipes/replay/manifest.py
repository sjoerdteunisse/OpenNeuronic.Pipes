"""ReplayManifest — describes exactly what to restore and how for a replay run."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReplayManifest:
    """Describes a replay or time-travel execution.

    Attributes:
        replay_point_id: The :attr:`~openneuronic.pipes.replay.point.ReplayPoint.replay_id`
            this manifest restores from.
        mode: A :class:`~openneuronic.pipes.core.enums.TimeTravelMode` value
            (``BOOKMARK``, ``RUN``, ``WINDOW``, or ``SNAPSHOT``).
        scope: Scope constraints for ``WINDOW`` or ``PARTIAL`` modes (free-form dict).
        source_snapshot: Snapshot of the source state at the time of capture.
        sink_strategy: A :class:`~openneuronic.pipes.core.enums.ReplayWriteStrategy`
            value controlling how the replay output is written
            (``dry_run``, ``shadow_write``, ``staging_compare``,
            ``replace_scope``, ``replace_full``).
        run_id: Unique identifier for this specific replay execution.
        reason: Human-readable reason for the replay (optional).
    """

    replay_point_id: str
    mode: str                   # TimeTravelMode value
    scope: dict[str, Any]
    source_snapshot: dict[str, Any]
    sink_strategy: str           # ReplayWriteStrategy value
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    reason: str | None = None
