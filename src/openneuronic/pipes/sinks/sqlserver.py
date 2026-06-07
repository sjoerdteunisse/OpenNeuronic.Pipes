from __future__ import annotations

from typing import Any, TYPE_CHECKING

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sinks._base import AbstractSink

if TYPE_CHECKING:
    from openneuronic.pipes.core.partial_scope import PartialScope


class SQLServerSink(AbstractSink):
    """Async SQL Server sink backed by aioodbc.

    Supports plain ``INSERT`` or a ``MERGE`` (upsert) statement when
    *upsert_key* is provided.  When *auto_migrate* is ``True`` the sink
    will create the target table from schema DDL (call :meth:`apply_schema`).

    Full-load staging:
        ``prepare_full_load()`` creates ``{table}_staging`` and truncates it.
        ``write()`` targets the staging table while a full load is in progress.
        ``commit_full_load()`` swaps staging → live using ``sp_rename``.

    Partial-load scope deletion:
        ``delete_scope(scope)`` deletes matching rows before replacement writes.
    """

    def __init__(
        self,
        connection: str,
        target_table: str,
        upsert_key: str | list[str] | None = None,
        auto_migrate: bool = False,
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
        self._auto_migrate = auto_migrate
        self._conn: Any | None = None
        self._staging: bool = False

    @property
    def _write_table(self) -> str:
        return f"{self._table}_staging" if self._staging else self._table

    async def setup(self) -> None:
        import aioodbc
        self._conn = await aioodbc.connect(dsn=self._dsn, autocommit=False)

    def _build_insert_sql(self, columns: list[str]) -> str:
        t = self._write_table
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        return f"INSERT INTO [{t}] ({col_list}) VALUES ({placeholders})"

    def _build_merge_sql(self, columns: list[str]) -> str:
        t = self._write_table
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
            f"MERGE [{t}] AS target "
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
        pass

    async def teardown(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Full-load staging
    # ------------------------------------------------------------------

    async def prepare_full_load(self) -> None:
        """Create and truncate ``[{table}_staging]``, redirect writes there."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        staging = f"{self._table}_staging"
        ddl = (
            f"IF OBJECT_ID('[dbo].[{staging}]', 'U') IS NULL "
            f"SELECT TOP 0 * INTO [{staging}] FROM [{self._table}];"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(ddl)
            await cur.execute(f"TRUNCATE TABLE [{staging}]")
        await self._conn.commit()
        self._staging = True

    async def commit_full_load(self) -> None:
        """Swap ``[{table}_staging]`` → ``[{table}]`` using sp_rename."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        staging = f"{self._table}_staging"
        old = f"{self._table}_old"
        async with self._conn.cursor() as cur:
            # Rename live → old, staging → live, then drop old
            await cur.execute(f"EXEC sp_rename '[{self._table}]', '{old}'")
            await cur.execute(f"EXEC sp_rename '[{staging}]', '{self._table}'")
            await cur.execute(
                f"IF OBJECT_ID('[dbo].[{old}]', 'U') IS NOT NULL "
                f"DROP TABLE [{old}]"
            )
        await self._conn.commit()
        self._staging = False

    # ------------------------------------------------------------------
    # Partial scope deletion
    # ------------------------------------------------------------------

    async def delete_scope(self, scope: PartialScope) -> None:
        """Delete rows matching *scope* from the live table."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        where = scope.to_where_clause()
        if not where:
            return
        sql = f"DELETE FROM [{self._table}] WHERE {where}"
        async with self._conn.cursor() as cur:
            await cur.execute(sql)
        await self._conn.commit()

    # ------------------------------------------------------------------
    # Auto DDL / migration ledger
    # ------------------------------------------------------------------

    async def apply_schema(self, schema_cls: type) -> None:
        """Create the target table from *schema_cls* DDL and record the migration."""
        if self._conn is None:
            raise RuntimeError("SQLServerSink.setup() must be called first")
        from openneuronic.pipes.schema.mapper.sqlserver import map_field
        import hashlib

        fields = schema_cls.__schema_fields__
        pk_fields = [n for n, f in fields.items() if f.primary_key]
        col_ddl = [map_field(name, f) for name, f in fields.items()]
        if pk_fields:
            pk = ", ".join(f"[{c}]" for c in pk_fields)
            col_ddl.append(f"CONSTRAINT [pk_{self._table}] PRIMARY KEY ({pk})")
        columns_sql = ",\n    ".join(col_ddl)
        ddl = (
            f"IF OBJECT_ID('[dbo].[{self._table}]', 'U') IS NULL "
            f"CREATE TABLE [{self._table}] (\n    {columns_sql}\n)"
        )

        checksum = hashlib.sha256(ddl.encode()).hexdigest()[:16]
        await self.ensure_migration_ledger()
        async with self._conn.cursor() as cur:
            await cur.execute(ddl)
        await self._conn.commit()
        await self.record_migration(
            schema_cls.__name__, schema_cls.__schema_version__, checksum
        )

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
