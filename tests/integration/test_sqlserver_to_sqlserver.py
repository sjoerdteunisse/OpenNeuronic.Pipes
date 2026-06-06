"""SQL Server → SQL Server integration test.

Copies ``dbo.Categories`` from the *now* database to ``dbo.Categories`` in
the *test* database using :class:`~openneuronic.pipes.sources.sqlserver.SQLServerSource`,
:class:`~openneuronic.pipes.sinks.sqlserver.SQLServerSink`, and
:class:`~openneuronic.pipes.runner.LocalRunner`.

Running
-------
These tests hit a real SQL Server instance and are **skipped by default**.
To enable them, set the following environment variables before running pytest::

    set ONPIPES_SS_USER=your_login
    set ONPIPES_SS_PASSWORD=your_password
    # Optional overrides (defaults shown):
    set ONPIPES_SS_SERVER=localhost
    set ONPIPES_SS_DRIVER=ODBC Driver 18 for SQL Server

Then run::

    py -3.14 -m pytest tests/integration -v -m integration

The SQL Server user requires:
    - SELECT on [now].[dbo].[Categories]
    - SELECT, INSERT, UPDATE, DELETE, CREATE TABLE on [test].[dbo]
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os

import pytest

try:
    import aioodbc
    _aioodbc_available = True
except ImportError:
    _aioodbc_available = False

from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.metrics.base import MeasureSet, MeasureSetRef, measure_set_registry
from openneuronic.pipes.metrics.measures.runtime import RuntimeMeasure
from openneuronic.pipes.runner import LocalRunner
from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType
from openneuronic.pipes.sinks.sqlserver import SQLServerSink
from openneuronic.pipes.sources.sqlserver import SQLServerSource

# Register the runtime measure set used by this test.
measure_set_registry.register(MeasureSet("core-runtime", [RuntimeMeasure()]))

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.integration

_SKIP_NO_AIOODBC = pytest.mark.skipif(
    not _aioodbc_available,
    reason="aioodbc is not installed (pip install openneuronic-pipes[sqlserver])",
)

_SKIP_NO_CREDS = pytest.mark.skipif(
    not (
        os.getenv("ONPIPES_SS_USER", "sa")
        and os.getenv("ONPIPES_SS_PASSWORD", "YourStrong!Passw0rd")
    ),
    reason=(
        "SQL Server credentials not set. "
        "Export ONPIPES_SS_USER and ONPIPES_SS_PASSWORD to run integration tests."
    ),
)


def _dsn(database: str) -> str:
    server = os.getenv("ONPIPES_SS_SERVER", "127.0.0.1")
    driver = os.getenv("ONPIPES_SS_DRIVER", "ODBC Driver 17 for SQL Server")
    user   = os.getenv("ONPIPES_SS_USER", "sa")
    pwd    = os.getenv("ONPIPES_SS_PASSWORD", "YourStrong!Passw0rd")
    return (
        f"DRIVER={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"UID={user};"
        f"PWD={pwd};"
        "TrustServerCertificate=yes;"
    )


# ---------------------------------------------------------------------------
# Abstract schema for dbo.Categories (Northwind)
# ---------------------------------------------------------------------------

@schema_version(1)
class CategoriesSchema(Schema):
    CategoryID   = Field(FieldType.INTEGER, nullable=False, primary_key=True)
    CategoryName = Field(FieldType.STRING,  max_length=15, nullable=False)
    Description  = Field(FieldType.TEXT,    nullable=True)
    Picture      = Field(FieldType.BYTES,   nullable=True)


# ---------------------------------------------------------------------------
# Source query
# ---------------------------------------------------------------------------

_SOURCE_QUERY = """
    SELECT TOP (1000)
        [CategoryID],
        [CategoryName],
        [Description],
        [Picture]
    FROM [now].[dbo].[Categories]
""".strip()


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------

@_SKIP_NO_AIOODBC
@_SKIP_NO_CREDS
async def test_copy_categories_sqlserver_to_sqlserver() -> None:
    """End-to-end copy of dbo.Categories from 'now' to 'test' via LocalRunner."""
    # Ensure the destination table exists before running the pipe.
    ddl_conn = await aioodbc.connect(dsn=_dsn("test"), autocommit=True)
    try:
        async with ddl_conn.cursor() as cur:
            from openneuronic.pipes.schema.mapper.sqlserver import create_table_ddl
            ddl = create_table_ddl("Categories", CategoriesSchema, if_not_exists=True)
            await cur.execute(ddl)
    finally:
        await ddl_conn.close()

    source = SQLServerSource(
        connection=_dsn("now"),
        query=_SOURCE_QUERY,
        source_id="sqlserver.now.Categories",
    )
    sink = SQLServerSink(
        connection=_dsn("test"),
        target_table="Categories",
        upsert_key="CategoryID",
    )
    pipe = Pipe(
        id="categories-now-to-test",
        source=source,
        sink=sink,
        schema=CategoriesSchema,
        mode=CopyMode.FULL,
        measures=[MeasureSetRef("core-runtime")],
    )

    result = await LocalRunner(batch_size=1000).run(pipe)

    assert result.success, f"Pipe failed: {result.error}"
    assert result.records_read > 0, "Source returned no records — is 'now.dbo.Categories' populated?"
    assert result.records_written == result.records_read, (
        f"Expected {result.records_read} records written, "
        f"got {result.records_written} (dropped={result.records_dropped})"
    )

    # --- Metrics assertions ---
    assert result.metrics is not None, "Metrics should be populated when measures are attached"
    m = result.metrics
    assert m.records_read == result.records_read
    assert m.records_written == result.records_written
    assert m.duration_ms > 0, "duration_ms must be positive"
    assert m.throughput_rps > 0, "throughput_rps must be positive"
    assert m.errors == 0

    log = logging.getLogger(__name__)
    log.info("RunMetrics:\n%s", json.dumps(dataclasses.asdict(m), indent=2, default=str))

    # Verify the count in the destination matches.
    conn = await aioodbc.connect(dsn=_dsn("test"), autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM [dbo].[Categories]")
            row = await cur.fetchone()
            dest_count = row[0]
    finally:
        await conn.close()

    # With upsert semantics the dest count may be ≥ records written (prior rows).
    assert dest_count >= result.records_written, (
        f"Destination has {dest_count} rows but expected at least {result.records_written}"
    )


# ---------------------------------------------------------------------------
# Performance test — 10 k synthetic rows seeded in source, then copied to dest
# ---------------------------------------------------------------------------

_PERF_TABLE = "PerfSeed"
_PERF_ROWS   = 10_000

_DROP_PERF  = f"IF OBJECT_ID('[dbo].[{_PERF_TABLE}]', 'U') IS NOT NULL DROP TABLE [dbo].[{_PERF_TABLE}]"
_CREATE_PERF = f"""
CREATE TABLE [dbo].[{_PERF_TABLE}] (
    [id]      INT          NOT NULL PRIMARY KEY,
    [name]    NVARCHAR(100) NOT NULL,
    [value]   FLOAT        NOT NULL,
    [payload] NVARCHAR(255) NULL
)
""".strip()

_SELECT_PERF = f"SELECT [id], [name], [value], [payload] FROM [test].[dbo].[{_PERF_TABLE}]"


@schema_version(1)
class PerfSeedSchema(Schema):
    id      = Field(FieldType.INTEGER, nullable=False, primary_key=True)
    name    = Field(FieldType.STRING,  max_length=100, nullable=False)
    value   = Field(FieldType.FLOAT,   nullable=False)
    payload = Field(FieldType.STRING,  max_length=255, nullable=True)


async def _setup_perf_tables() -> None:
    """Create and populate [test].[dbo].[PerfSeed] with _PERF_ROWS rows."""
    conn = await aioodbc.connect(dsn=_dsn("test"), autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(_DROP_PERF)
            await cur.execute(_CREATE_PERF)
            # Bulk insert via executemany in chunks of 1000
            chunk = 1000
            rows = [
                (i, f"record_{i}", float(i) * 1.1, f"payload_{i}" if i % 3 != 0 else None)
                for i in range(1, _PERF_ROWS + 1)
            ]
            for offset in range(0, len(rows), chunk):
                batch = rows[offset : offset + chunk]
                await cur.executemany(
                    f"INSERT INTO [dbo].[{_PERF_TABLE}] ([id],[name],[value],[payload]) "
                    "VALUES (?,?,?,?)",
                    batch,
                )
    finally:
        await conn.close()


async def _teardown_perf_table(database: str) -> None:
    conn = await aioodbc.connect(dsn=_dsn(database), autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(_DROP_PERF)
    finally:
        await conn.close()


@_SKIP_NO_AIOODBC
@_SKIP_NO_CREDS
async def test_copy_10k_rows_performance() -> None:
    """Seed 10 000 rows in [test].[dbo].[PerfSeed], copy them to a second table, check metrics."""
    log = logging.getLogger(__name__)

    await _setup_perf_tables()

    # Destination table in a separate table within the same test DB.
    _DEST_TABLE = f"{_PERF_TABLE}_dest"
    _DROP_DEST  = f"IF OBJECT_ID('[dbo].[{_DEST_TABLE}]', 'U') IS NOT NULL DROP TABLE [dbo].[{_DEST_TABLE}]"

    ddl_conn = await aioodbc.connect(dsn=_dsn("test"), autocommit=True)
    try:
        async with ddl_conn.cursor() as cur:
            await cur.execute(_DROP_DEST)
            from openneuronic.pipes.schema.mapper.sqlserver import create_table_ddl
            ddl = create_table_ddl(_DEST_TABLE, PerfSeedSchema, if_not_exists=True)
            await cur.execute(ddl)
    finally:
        await ddl_conn.close()

    source = SQLServerSource(
        connection=_dsn("test"),
        query=_SELECT_PERF,
        source_id=f"sqlserver.test.{_PERF_TABLE}",
    )
    sink = SQLServerSink(
        connection=_dsn("test"),
        target_table=_DEST_TABLE,
        upsert_key="id",
    )

    # Register a named measure set for this test (idempotent guard).
    _MS_NAME = "perf-runtime"
    if _MS_NAME not in measure_set_registry._store:
        measure_set_registry.register(MeasureSet(_MS_NAME, [RuntimeMeasure()]))

    pipe = Pipe(
        id="perf-seed-copy",
        source=source,
        sink=sink,
        schema=PerfSeedSchema,
        mode=CopyMode.FULL,
        measures=[MeasureSetRef(_MS_NAME)],
    )

    result = await LocalRunner(batch_size=1000).run(pipe)

    try:
        assert result.success, f"Pipe failed: {result.error}"
        assert result.records_read == _PERF_ROWS, (
            f"Expected {_PERF_ROWS} records read, got {result.records_read}"
        )
        assert result.records_written == _PERF_ROWS, (
            f"Expected {_PERF_ROWS} records written, got {result.records_written}"
        )

        m = result.metrics
        assert m is not None
        assert m.duration_ms > 0
        assert m.throughput_rps > 0

        log.info(
            "Performance run — %d rows | duration=%.1f ms | throughput=%.0f rows/s | batches=%d",
            m.records_written,
            m.duration_ms,
            m.throughput_rps,
            m.batches,
        )
        log.info("RunMetrics:\n%s", json.dumps(dataclasses.asdict(m), indent=2, default=str))
    finally:
        # Clean up the temporary tables regardless of pass/fail.
        await _teardown_perf_table("test")
        ddl_conn2 = await aioodbc.connect(dsn=_dsn("test"), autocommit=True)
        try:
            async with ddl_conn2.cursor() as cur:
                await cur.execute(
                    f"IF OBJECT_ID('[dbo].[{_DEST_TABLE}]', 'U') IS NOT NULL "
                    f"DROP TABLE [dbo].[{_DEST_TABLE}]"
                )
        finally:
            await ddl_conn2.close()
