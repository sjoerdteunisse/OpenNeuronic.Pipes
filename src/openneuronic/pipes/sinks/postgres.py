from __future__ import annotations

from typing import Any, TYPE_CHECKING

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sinks._base import AbstractSink

if TYPE_CHECKING:
    from openneuronic.pipes.core.partial_scope import PartialScope


class PostgresSink(AbstractSink):
    """Async PostgreSQL sink backed by asyncpg.

    Supports plain ``INSERT`` or ``INSERT … ON CONFLICT … DO UPDATE`` (upsert)
    when *upsert_key* is provided.  When *auto_migrate* is ``True`` the sink
    will create the target table from the schema DDL on first :meth:`setup`
    (call :meth:`apply_schema` from the runner or manually).

    Full-load staging:
        ``prepare_full_load()`` creates ``{table}_staging`` and truncates it.
        ``write()`` targets the staging table while a full load is in progress.
        ``commit_full_load()`` atomically swaps staging → live with a DDL rename.

    Partial-load scope deletion:
        ``delete_scope(scope)`` deletes rows matching the :class:`PartialScope`
        before replacement data is written.
    """

    def __init__(
        self,
        connection: str,
        target_table: str,
        upsert_key: str | list[str] | None = None,
        auto_migrate: bool = False,
    ) -> None:
        try:
            import asyncpg  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "asyncpg is required for PostgresSink. "
                "Install with: pip install 'openneuronic-pipes[postgres]'"
            ) from exc
        self._dsn = connection
        self._table = target_table
        self._upsert_key: list[str] = (
            [upsert_key] if isinstance(upsert_key, str) else (upsert_key or [])
        )
        self._auto_migrate = auto_migrate
        self._pool: Any | None = None
        self._staging: bool = False  # True while a full-load is in progress

    @property
    def _write_table(self) -> str:
        return f"{self._table}_staging" if self._staging else self._table

    async def setup(self) -> None:
        import asyncpg
        self._pool = await asyncpg.create_pool(self._dsn)

    def _build_sql(self, columns: list[str], table: str | None = None) -> str:
        t = table or self._write_table
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

        if not self._upsert_key:
            return f'INSERT INTO "{t}" ({col_list}) VALUES ({placeholders})'

        conflict_cols = ", ".join(f'"{c}"' for c in self._upsert_key)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in columns
            if c not in self._upsert_key
        )
        return (
            f'INSERT INTO "{t}" ({col_list}) VALUES ({placeholders}) '
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        )

    async def write(self, records: list[Record]) -> None:
        if not records:
            return
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called before write()")

        columns = list(records[0].payload.keys())
        sql = self._build_sql(columns)
        rows = [[r.payload[c] for c in columns] for r in records]

        async with self._pool.acquire() as conn:
            await conn.executemany(sql, rows)

    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        pass

    async def teardown(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------
    # Full-load staging
    # ------------------------------------------------------------------

    async def prepare_full_load(self) -> None:
        """Create and truncate ``{table}_staging``, redirect writes there."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        staging = f"{self._table}_staging"
        async with self._pool.acquire() as conn:
            # Mirror the live table structure; DROP + re-CREATE is simpler than
            # TRUNCATE when the staging table may not yet exist.
            await conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{staging}" (LIKE "{self._table}" INCLUDING ALL)'
            )
            await conn.execute(f'TRUNCATE "{staging}"')
        self._staging = True

    async def commit_full_load(self) -> None:
        """Atomically swap ``{table}_staging`` → ``{table}``."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        staging = f"{self._table}_staging"
        old = f"{self._table}_old"
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f'ALTER TABLE "{self._table}" RENAME TO "{old}"')
                await conn.execute(f'ALTER TABLE "{staging}" RENAME TO "{self._table}"')
                await conn.execute(f'DROP TABLE IF EXISTS "{old}"')
        self._staging = False

    # ------------------------------------------------------------------
    # Partial scope deletion
    # ------------------------------------------------------------------

    async def delete_scope(self, scope: PartialScope) -> None:
        """Delete rows in the live table that match *scope* before writing
        replacement data."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        where = scope.to_where_clause()
        if not where:
            return
        sql = f'DELETE FROM "{self._table}" WHERE {where}'
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    # ------------------------------------------------------------------
    # Auto DDL / migration ledger
    # ------------------------------------------------------------------

    async def apply_schema(self, schema_cls: type) -> None:
        """Create the target table from *schema_cls* DDL and record the migration."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        from openneuronic.pipes.schema.base import Schema
        from openneuronic.pipes.schema.mapper.postgres import map_field
        import hashlib

        fields = schema_cls.__schema_fields__
        pk_fields = [n for n, f in fields.items() if f.primary_key]
        col_ddl = [map_field(name, f) for name, f in fields.items()]
        if pk_fields:
            pk = ", ".join(f'"{c}"' for c in pk_fields)
            col_ddl.append(f"PRIMARY KEY ({pk})")
        columns_sql = ",\n    ".join(col_ddl)
        ddl = f'CREATE TABLE IF NOT EXISTS "{self._table}" (\n    {columns_sql}\n)'

        checksum = hashlib.sha256(ddl.encode()).hexdigest()[:16]
        await self.ensure_migration_ledger()
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)
        await self.record_migration(
            schema_cls.__name__, schema_cls.__schema_version__, checksum
        )

    async def ensure_migration_ledger(self) -> None:
        """Create ``on_schema_migrations`` in the target database if absent."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        ddl = """
            CREATE TABLE IF NOT EXISTS on_schema_migrations (
                schema_name TEXT        NOT NULL,
                version     INTEGER     NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                checksum    TEXT,
                PRIMARY KEY (schema_name, version)
            )
        """
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)

    async def record_migration(
        self, schema_name: str, version: int, checksum: str | None = None
    ) -> None:
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        sql = """
            INSERT INTO on_schema_migrations (schema_name, version, checksum)
            VALUES ($1, $2, $3)
            ON CONFLICT (schema_name, version) DO NOTHING
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, schema_name, version, checksum)
