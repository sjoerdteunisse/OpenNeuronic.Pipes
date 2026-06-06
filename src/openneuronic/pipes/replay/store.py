"""ReplayStore — abstract and in-memory implementations."""
from __future__ import annotations

import dataclasses
import datetime
import json
from abc import ABC, abstractmethod
from typing import Any

from openneuronic.pipes.replay.point import ReplayPoint


class ReplayStore(ABC):
    """Abstract persistence layer for :class:`~openneuronic.pipes.replay.point.ReplayPoint`
    objects.

    The Redis-backed implementation is added in Milestone 5.
    """

    @abstractmethod
    async def save(self, point: ReplayPoint) -> None:
        """Persist *point*, keyed by :attr:`~ReplayPoint.replay_id`."""

    @abstractmethod
    async def load(self, replay_id: str) -> ReplayPoint | None:
        """Return the :class:`ReplayPoint` for *replay_id*, or ``None``."""

    @abstractmethod
    async def list_for_pipe(self, pipe_id: str) -> list[ReplayPoint]:
        """Return all replay points stored for *pipe_id*, newest-first."""


class InMemoryReplayStore(ReplayStore):
    """Thread-unsafe in-process store suitable for tests and local development."""

    def __init__(self) -> None:
        self._store: dict[str, ReplayPoint] = {}

    async def save(self, point: ReplayPoint) -> None:
        self._store[point.replay_id] = point

    async def load(self, replay_id: str) -> ReplayPoint | None:
        return self._store.get(replay_id)

    async def list_for_pipe(self, pipe_id: str) -> list[ReplayPoint]:
        results = [p for p in self._store.values() if p.pipe_id == pipe_id]
        return sorted(results, key=lambda p: p.created_at, reverse=True)


def _point_to_dict(point: ReplayPoint) -> dict[str, Any]:
    d = dataclasses.asdict(point)
    d["created_at"] = d["created_at"].isoformat()
    return d


def _dict_to_point(d: dict[str, Any]) -> ReplayPoint:
    if isinstance(d.get("created_at"), str):
        d["created_at"] = datetime.datetime.fromisoformat(d["created_at"])
    return ReplayPoint(**d)


class RedisReplayStore(ReplayStore):
    """Redis-backed replay point store.

    Keys:
    - ``on:pipes:replay:{replay_id}`` — JSON-serialised :class:`ReplayPoint`.
    - ``on:pipes:pipe:{pipe_id}:replays`` — sorted set of ``replay_id`` values
      scored by Unix timestamp (for newest-first ordering).

    Requires the ``redis`` optional extra::

        pip install 'openneuronic-pipes[redis]'
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        try:
            import redis.asyncio  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "redis[asyncio] is required for RedisReplayStore. "
                "Install with: pip install 'openneuronic-pipes[redis]'"
            ) from exc
        self._url = redis_url
        self._client: Any | None = None

    async def _get_client(self) -> Any:
        if self._client is None:
            from redis.asyncio import Redis
            self._client = Redis.from_url(self._url, decode_responses=True)
        return self._client

    async def save(self, point: ReplayPoint) -> None:
        client = await self._get_client()
        key = f"on:pipes:replay:{point.replay_id}"
        await client.set(key, json.dumps(_point_to_dict(point)))
        if point.pipe_id:
            score = point.created_at.timestamp()
            await client.zadd(
                f"on:pipes:pipe:{point.pipe_id}:replays",
                {point.replay_id: score},
            )

    async def load(self, replay_id: str) -> ReplayPoint | None:
        client = await self._get_client()
        raw = await client.get(f"on:pipes:replay:{replay_id}")
        if raw is None:
            return None
        return _dict_to_point(json.loads(raw))

    async def list_for_pipe(self, pipe_id: str) -> list[ReplayPoint]:
        client = await self._get_client()
        # ZREVRANGE returns ids sorted by score descending (newest first)
        ids = await client.zrevrange(f"on:pipes:pipe:{pipe_id}:replays", 0, -1)
        results = []
        for rid in ids:
            point = await self.load(rid)
            if point is not None:
                results.append(point)
        return results
