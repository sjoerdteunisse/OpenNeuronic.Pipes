"""Durability helper — resume semantics for crashed Opus runs."""
from __future__ import annotations

from openneuronic.pipes.core.enums import SegmentStatus
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.opus.state import DurableRunState


async def filter_pending_segments(
    opus: Opus, state_store: DurableRunState
) -> list[PipeSegment]:
    """Return only the segments that still need to run.

    A segment is skipped when its persisted
    :class:`~openneuronic.pipes.opus.state.SegmentState` has
    :attr:`~openneuronic.pipes.core.enums.SegmentStatus.SUCCESS` status —
    it was already committed in a previous run attempt.

    All other segments (PENDING, FAILED, RETRYING, RUNNING, …) are included
    so the runner can re-execute or retry them.

    Args:
        opus: The opus whose segments to evaluate.
        state_store: Persisted run state for the current ``run_id``.

    Returns:
        Subset of ``opus.segments`` excluding already-successful segments.
    """
    pending: list[PipeSegment] = []
    for seg in opus.segments:
        state = await state_store.get_segment_state(seg.id)
        if state is None or state.status != SegmentStatus.SUCCESS:
            pending.append(seg)
    return pending
