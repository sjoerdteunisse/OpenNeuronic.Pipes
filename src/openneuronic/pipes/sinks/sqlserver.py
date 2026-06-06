from __future__ import annotations

from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sinks._base import AbstractSink


class SQLServerSink(AbstractSink):
    """Async SQL Server sink backed by aioodbc.

    Supports plain ``INSERT`` or a ``MERGE`` (upsert) statement when
    *upsert_key* is provided.

    Example::

        SQLServerSink(
            connection="DSN=mydsn;UID=user;PWD=pass",
            target_table="orders_clean",
            upsert_key="id",
        )
    """

    def __init__(
        self,
        connection: str,
        target_table: str,
        upsert_key: str | list[str] | None = None,
    ) -> None:
        try:
            import aioodbc  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aioodbc is required for SQLServerSink. "
                "Install with: pip install 'openneuronic-pipes[sqlserver]'"
            ) from exc
        self._dsn = connection
        self._table = target_table
        self._upsert_key: list[str] = (
            [upsert_key] if isinstance(upsert_key, str) else (upsert_key or [])
        )
        self._conn: Any | None = None

    async def setup(self) -> None:
        import aioodbc
        self._conn = await aioodbc.connect(dsn=self._dsn, autocommit=False)

    def _build_insert_sql(self, columns: list[str]) -> str:
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        return f"INSERT INTO [{self._table}] ({col_list}) VALUES ({placeholders})"

    def _build_merge_sql(self, columns: list[str]) -> str:
        # MERGE [{table}] AS target
        # USING (VALUES (?, ?, ...)) AS source ([col1], [col2], ...)
        # ON target.[key] = source.[key]
        # WHEN MATCHED THEN UPDATE SET target.[c] = source.[c], ...
        # WHEN NOT MATCHED THEN INSERT ([col1], ...) VALUES (source.[col1], ...);
        col_list = ", ".join(f"[{c}]" for c in columns)
        source_vals = "VALUES (" + ", ".join("?" for _ in columns) + ")"
        source_cols = ", ".join(f"[{c}]" for c in columns)
        join_cond = " AND ".join(
            f"target.[{c}] = source.[{c}]" for c in self._upsert_key
        )
        non_key = [c for c in columns if c not in self._upsert_key]
        update_set = ", ".join(f"target.[{c}] = source.[{c}]" for c in non_key)
        insert_vals = ", ".join(f"source.[{c}]" for c in columns)
        return (
            f"MERGE [{self._table}] AS target "
            f"USING ({source_vals}) AS source ({source_cols}) "
            f"ON ({join_cond}) "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals});"
        )

    async def write(self, records: list[Record]) -> None:
        if not records:
            return
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called before write()")

        columns = list(records[0].payload.keys())
        sql = (
            self._build_merge_sql(columns)
            if self._upsert_key
            else self._build_insert_sql(columns)
        )
        rows = [[r.payload[c] for c in columns] for r in records]

        async with self._conn.cursor() as cursor:
            await cursor.executemany(sql, rows)
        await self._conn.commit()

    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        # Bookmark persistence deferred to Milestone 5 (Redis).
        pass

    async def teardown(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Migration ledger
    # ------------------------------------------------------------------

    async def ensure_migration_ledger(self) -> None:
        """Create ``on_schema_migrations`` in the target database if absent."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        ddl = (
            "IF OBJECT_ID('[dbo].[on_schema_migrations]', 'U') IS NULL "
            "CREATE TABLE [dbo].[on_schema_migrations] ("
            "    [schema_name] NVARCHAR(255) NOT NULL,"
            "    [version]     INT           NOT NULL,"
            "    [applied_at]  DATETIMEOFFSET(7) NOT NULL "
            "        CONSTRAINT [df_osm_applied_at] DEFAULT SYSDATETIMEOFFSET(),"
            "    [checksum]    NVARCHAR(64)  NULL,"
            "    CONSTRAINT [pk_on_schema_migrations] PRIMARY KEY ([schema_name], [version])"
            ")"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(ddl)
        await self._conn.commit()

    async def record_migration(
        self, schema_name: str, version: int, checksum: str | None = None
    ) -> None:
        """Upsert a row in ``on_schema_migrations`` for *schema_name* at *version*."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        sql = (
            "MERGE [dbo].[on_schema_migrations] AS target "
            "USING (VALUES (?, ?, ?)) AS source ([schema_name], [version], [checksum]) "
            "ON (target.[schema_name] = source.[schema_name] AND target.[version] = source.[version]) "
            "WHEN NOT MATCHED THEN INSERT ([schema_name], [version], [checksum]) "
            "VALUES (source.[schema_name], source.[version], source.[checksum]);"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (schema_name, version, checksum))
        await self._conn.commit()
