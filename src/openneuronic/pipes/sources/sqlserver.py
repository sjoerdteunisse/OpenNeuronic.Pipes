from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sources._base import AbstractSource


class SQLServerSource(AbstractSource):
    """Async SQL Server source backed by aioodbc.

    The *query* should use ``?`` positional parameters (ODBC style).
    When a bookmark is supplied its value is passed as the **first** parameter.

    Example::

        SQLServerSource(
            connection="DSN=mydsn;UID=user;PWD=pass",
            query="SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at",
        )
    """

    def __init__(
        self,
        connection: str,
        query: str,
        source_id: str = "sqlserver",
    ) -> None:
        try:
            import aioodbc  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aioodbc is required for SQLServerSource. "
                "Install with: pip install 'openneuronic-pipes[sqlserver]'"
            ) from exc
        self._dsn = connection
        self._query = query
        self._source_id = source_id
        self._conn: Any | None = None

    async def setup(self) -> None:
        import aioodbc
        self._conn = await aioodbc.connect(dsn=self._dsn)

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        async def _generate() -> AsyncIterator[Record]:
            if self._conn is None:
                raise RuntimeError("SQLServerSource.setup() must be called before read()")
            params: tuple[Any, ...] = (bookmark.value,) if bookmark is not None else ()
            async with self._conn.cursor() as cursor:
                await cursor.execute(self._query, params)
                columns = [col[0] for col in cursor.description]
                async for row in cursor:
                    yield Record(
                        payload=dict(zip(columns, row)),
                        source_id=self._source_id,
                    )

        return _generate()

    async def teardown(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
