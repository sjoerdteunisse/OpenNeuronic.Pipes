from __future__ import annotations

from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sinks._base import AbstractSink


class PostgresSink(AbstractSink):
    """Async PostgreSQL sink backed by asyncpg.

    Supports plain ``INSERT`` or ``INSERT … ON CONFLICT … DO UPDATE`` (upsert)
    when *upsert_key* is provided.

    Example::

        PostgresSink(
            connection="postgresql://user:pass@host/db",
            target_table="orders_clean",
            upsert_key="id",
        )
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

    async def setup(self) -> None:
        import asyncpg
        self._pool = await asyncpg.create_pool(self._dsn)

    def _build_sql(self, columns: list[str]) -> str:
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

        if not self._upsert_key:
            return f'INSERT INTO "{self._table}" ({col_list}) VALUES ({placeholders})'

        conflict_cols = ", ".join(f'"{c}"' for c in self._upsert_key)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in columns
            if c not in self._upsert_key
        )
        return (
            f'INSERT INTO "{self._table}" ({col_list}) VALUES ({placeholders}) '
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
        # Bookmark persistence deferred to Milestone 5 (Redis).
        # In the local runner tier the Bookmark object is already updated
        # in-memory by the runner before this is called.
        pass

    async def teardown(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------
    # Migration ledger
    # ------------------------------------------------------------------

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
        """Upsert a row in ``on_schema_migrations`` for *schema_name* at *version*."""
        if self._pool is None:
            raise RuntimeError("PostgresSink.setup() must be called first")
        sql = """
            INSERT INTO on_schema_migrations (schema_name, version, checksum)
            VALUES ($1, $2, $3)
            ON CONFLICT (schema_name, version) DO NOTHING
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, schema_name, version, checksum)
