from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from openneuronic.pipes.core.enums import BookmarkType


@dataclass
class Bookmark:
    pipe_id: str
    column: str
    type: BookmarkType
    value: Any
    previous_value: Any = None
    batch_count: int = 0


class BookmarkStore(ABC):
    """Abstract store for persisting and loading :class:`Bookmark` state.

    The Redis implementation is added in Milestone 5.  The in-process
    :class:`InMemoryBookmarkStore` is used by default in ``LocalRunner``.
    """

    @abstractmethod
    async def save(self, bookmark: Bookmark) -> None:
        """Persist *bookmark* keyed by ``pipe_id``."""

    @abstractmethod
    async def load(self, pipe_id: str) -> Bookmark | None:
        """Return the stored bookmark for *pipe_id*, or ``None``."""

    async def delete(self, pipe_id: str) -> None:  # noqa: B027  (intentional no-op default)
        """Remove the stored bookmark. Default is a no-op; override when supported."""


class InMemoryBookmarkStore(BookmarkStore):
    """In-process bookmark store for local development and tests."""

    def __init__(self) -> None:
        self._store: dict[str, Bookmark] = {}

    async def save(self, bookmark: Bookmark) -> None:
        self._store[bookmark.pipe_id] = bookmark

    async def load(self, pipe_id: str) -> Bookmark | None:
        return self._store.get(pipe_id)

    async def delete(self, pipe_id: str) -> None:
        self._store.pop(pipe_id, None)

class RedisBookmarkStore(BookmarkStore):
    """Redis-backed bookmark store.

    Persists bookmarks under the key ``on:pipes:bookmark:{pipe_id}`` as
    JSON, following the Redis key convention from the project spec.

    Requires the ``redis`` optional extra::

        pip install 'openneuronic-pipes[redis]'

    Args:
        redis_url: Redis connection URL, e.g. ``"redis://localhost:6379/0"``.
        key_prefix: Override the default key prefix ``"on:pipes:bookmark"``.
    """

    _PREFIX = "on:pipes:bookmark"

    def __init__(self, redis_url: str = "redis://localhost:6379/0", key_prefix: str | None = None) -> None:
        try:
            import redis.asyncio  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "redis[asyncio] is required for RedisBookmarkStore. "
                "Install with: pip install 'openneuronic-pipes[redis]'"
            ) from exc
        self._url = redis_url
        self._prefix = key_prefix or self._PREFIX
        self._client: Any | None = None

    async def _get_client(self) -> Any:
        if self._client is None:
            from redis.asyncio import Redis
            self._client = Redis.from_url(self._url, decode_responses=True)
        return self._client

    def _key(self, pipe_id: str) -> str:
        return f"{self._prefix}:{pipe_id}"

    async def save(self, bookmark: Bookmark) -> None:
        import json
        client = await self._get_client()
        data = {
            "pipe_id": bookmark.pipe_id,
            "column": bookmark.column,
            "type": str(bookmark.type),
            "value": str(bookmark.value) if bookmark.value is not None else None,
            "previous_value": str(bookmark.previous_value) if bookmark.previous_value is not None else None,
            "batch_count": bookmark.batch_count,
        }
        await client.set(self._key(bookmark.pipe_id), json.dumps(data))

    async def load(self, pipe_id: str) -> Bookmark | None:
        import json
        client = await self._get_client()
        raw = await client.get(self._key(pipe_id))
        if raw is None:
            return None
        d = json.loads(raw)
        return Bookmark(
            pipe_id=d["pipe_id"],
            column=d["column"],
            type=BookmarkType(d["type"]),
            value=d["value"],
            previous_value=d.get("previous_value"),
            batch_count=d.get("batch_count", 0),
        )

    async def delete(self, pipe_id: str) -> None:
        client = await self._get_client()
        await client.delete(self._key(pipe_id))