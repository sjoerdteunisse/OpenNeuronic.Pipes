"""Durable run state — per-segment lifecycle tracking for an Opus run."""
from __future__ import annotations

import datetime
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from openneuronic.pipes.core.enums import SegmentStatus


@dataclass
class SegmentState:
    """Runtime state for one :class:`~openneuronic.pipes.opus.segment.PipeSegment`
    within an :class:`~openneuronic.pipes.opus.opus.Opus` run.

    Attributes:
        segment_id: Matches :attr:`PipeSegment.id`.
        status: Current lifecycle status.
        retry_count: Number of retry attempts so far.
        started_at: UTC timestamp when the segment last transitioned to RUNNING.
        finished_at: UTC timestamp when the segment reached a terminal state.
        error: Serialised error message on failure, else ``None``.
    """

    segment_id: str
    status: SegmentStatus = SegmentStatus.PENDING
    retry_count: int = 0
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    error: str | None = None


class DurableRunState(ABC):
    """Abstract durable state store for an Opus run.

    One instance represents the state of a single ``(opus_id, run_id)`` pair.
    """

    @abstractmethod
    async def get_segment_state(self, segment_id: str) -> SegmentState | None:
        """Return the current :class:`SegmentState` for *segment_id*, or ``None``."""

    @abstractmethod
    async def set_segment_state(self, state: SegmentState) -> None:
        """Persist *state*."""

    @abstractmethod
    async def get_all_states(self) -> list[SegmentState]:
        """Return all persisted :class:`SegmentState` objects for this run."""

    @abstractmethod
    async def update_heartbeat(self) -> None:
        """Record the current UTC time as the last heartbeat."""

    @abstractmethod
    async def get_heartbeat(self) -> datetime.datetime | None:
        """Return the last recorded heartbeat, or ``None``."""


class InMemoryDurableRunState(DurableRunState):
    """In-process durable run state for tests and local development.

    Args:
        opus_id: Owning opus identifier.
        run_id: Unique run identifier.
    """

    def __init__(self, opus_id: str, run_id: str) -> None:
        self.opus_id = opus_id
        self.run_id = run_id
        self._states: dict[str, SegmentState] = {}
        self._heartbeat: datetime.datetime | None = None

    async def get_segment_state(self, segment_id: str) -> SegmentState | None:
        return self._states.get(segment_id)

    async def set_segment_state(self, state: SegmentState) -> None:
        self._states[state.segment_id] = state

    async def get_all_states(self) -> list[SegmentState]:
        return list(self._states.values())

    async def update_heartbeat(self) -> None:
        self._heartbeat = datetime.datetime.now(datetime.UTC)

    async def get_heartbeat(self) -> datetime.datetime | None:
        return self._heartbeat


class RedisDurableRunState(DurableRunState):
    """Redis-backed durable run state.

    Key pattern (matching the project spec)::

        on:pipes:opus:{opus_id}:run:{run_id}:segment:{segment_id}
        on:pipes:opus:{opus_id}:run:{run_id}:heartbeat

    Requires the ``redis`` optional extra::

        pip install 'openneuronic-pipes[redis]'
    """

    def __init__(
        self, opus_id: str, run_id: str, redis_url: str = "redis://localhost:6379/0"
    ) -> None:
        try:
            import redis.asyncio  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "redis[asyncio] is required for RedisDurableRunState. "
                "Install with: pip install 'openneuronic-pipes[redis]'"
            ) from exc
        self.opus_id = opus_id
        self.run_id = run_id
        self._url = redis_url
        self._client: Any | None = None

    async def _get_client(self) -> Any:
        if self._client is None:
            from redis.asyncio import Redis
            self._client = Redis.from_url(self._url, decode_responses=True)
        return self._client

    def _seg_key(self, segment_id: str) -> str:
        return f"on:pipes:opus:{self.opus_id}:run:{self.run_id}:segment:{segment_id}"

    def _hb_key(self) -> str:
        return f"on:pipes:opus:{self.opus_id}:run:{self.run_id}:heartbeat"

    def _index_key(self) -> str:
        return f"on:pipes:opus:{self.opus_id}:run:{self.run_id}:segments"

    @staticmethod
    def _encode(state: SegmentState) -> str:
        d: dict[str, Any] = {
            "segment_id": state.segment_id,
            "status": str(state.status),
            "retry_count": state.retry_count,
            "started_at": state.started_at.isoformat() if state.started_at else None,
            "finished_at": state.finished_at.isoformat() if state.finished_at else None,
            "error": state.error,
        }
        return json.dumps(d)

    @staticmethod
    def _decode(raw: str) -> SegmentState:
        d = json.loads(raw)
        return SegmentState(
            segment_id=d["segment_id"],
            status=SegmentStatus(d["status"]),
            retry_count=d.get("retry_count", 0),
            started_at=datetime.datetime.fromisoformat(d["started_at"]) if d.get("started_at") else None,
            finished_at=datetime.datetime.fromisoformat(d["finished_at"]) if d.get("finished_at") else None,
            error=d.get("error"),
        )

    async def get_segment_state(self, segment_id: str) -> SegmentState | None:
        client = await self._get_client()
        raw = await client.get(self._seg_key(segment_id))
        return self._decode(raw) if raw else None

    async def set_segment_state(self, state: SegmentState) -> None:
        client = await self._get_client()
        await client.set(self._seg_key(state.segment_id), self._encode(state))
        await client.sadd(self._index_key(), state.segment_id)

    async def get_all_states(self) -> list[SegmentState]:
        client = await self._get_client()
        ids = await client.smembers(self._index_key())
        results = []
        for sid in ids:
            state = await self.get_segment_state(sid)
            if state is not None:
                results.append(state)
        return results

    async def update_heartbeat(self) -> None:
        client = await self._get_client()
        await client.set(self._hb_key(), datetime.datetime.now(datetime.UTC).isoformat())

    async def get_heartbeat(self) -> datetime.datetime | None:
        client = await self._get_client()
        raw = await client.get(self._hb_key())
        return datetime.datetime.fromisoformat(raw) if raw else None
