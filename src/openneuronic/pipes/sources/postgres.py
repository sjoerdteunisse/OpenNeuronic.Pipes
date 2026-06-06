from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sources._base import AbstractSource


class PostgresSource(AbstractSource):
    """Async PostgreSQL source backed by asyncpg.

    The *query* must use asyncpg positional parameters (``$1``, ``$2``, …).
    When a bookmark is supplied its value is passed as the **first** positional
    argument (``$1``).  For a full-reload query that takes no parameters, omit
    the bookmark or pass ``None``.

    Example::

        PostgresSource(
            connection="postgresql://user:pass@host/db",
            query="SELECT * FROM orders WHERE updated_at > $1 ORDER BY updated_at",
        )
    """

    def __init__(
        self,
        connection: str,
        query: str,
        source_id: str = "postgres",
    ) -> None:
        try:
            import asyncpg  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "asyncpg is required for PostgresSource. "
                "Install with: pip install 'openneuronic-pipes[postgres]'"
            ) from exc
        self._dsn = connection
        self._query = query
        self._source_id = source_id
        self._pool: Any | None = None

    async def setup(self) -> None:
        import asyncpg
        self._pool = await asyncpg.create_pool(self._dsn)

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        async def _generate() -> AsyncIterator[Record]:
            if self._pool is None:
                raise RuntimeError("PostgresSource.setup() must be called before read()")
            args: tuple[Any, ...] = (bookmark.value,) if bookmark is not None else ()
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    async for row in conn.cursor(self._query, *args):
                        yield Record(
                            payload=dict(row),
                            source_id=self._source_id,
                        )

        return _generate()

    async def teardown(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
