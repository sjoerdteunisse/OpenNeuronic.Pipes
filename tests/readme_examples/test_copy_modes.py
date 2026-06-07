"""README example tests — Copy Modes section."""
from __future__ import annotations

import datetime
from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import CopyMode, PartialScope, Pipe, Record
from openneuronic.pipes import LocalRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int = 5) -> None:
        self._n = n

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _g():
            for i in range(n):
                yield Record(payload={"id": i, "name": f"item-{i}"})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self.prepared = False
        self.committed = False
        self.deleted_scope: PartialScope | None = None
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass

    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)

    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass

    async def prepare_full_load(self) -> None:
        self.prepared = True

    async def commit_full_load(self) -> None:
        self.committed = True

    async def delete_scope(self, scope: PartialScope) -> None:
        self.deleted_scope = scope


# ---------------------------------------------------------------------------
# Tests — FULL mode
# ---------------------------------------------------------------------------

async def test_full_mode_calls_prepare_and_commit() -> None:
    sink = _CaptureSink()
    pipe = Pipe(id="ref-data", source=_CountSource(5), sink=sink, mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)

    assert result.success
    assert sink.prepared
    assert sink.committed
    assert result.records_written == 5


# ---------------------------------------------------------------------------
# Tests — INCREMENTAL mode
# ---------------------------------------------------------------------------

async def test_incremental_mode_writes_all_records() -> None:
    sink = _CaptureSink()
    pipe = Pipe(id="inc", source=_CountSource(3), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe)

    assert result.success
    assert result.records_written == 3
    assert not sink.prepared      # no staging swap for incremental
    assert not sink.committed


# ---------------------------------------------------------------------------
# Tests — PARTIAL mode
# ---------------------------------------------------------------------------

async def test_partial_mode_calls_delete_scope() -> None:
    scope = PartialScope(
        date_column="created_at",
        date_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        date_end=datetime.datetime(2024, 1, 31, tzinfo=datetime.UTC),
    )
    sink = _CaptureSink()
    pipe = Pipe(id="backfill", source=_CountSource(4), sink=sink, mode=CopyMode.PARTIAL, scope=scope)
    result = await LocalRunner().run(pipe)

    assert result.success
    assert sink.deleted_scope is scope
    assert result.records_written == 4


# ---------------------------------------------------------------------------
# Tests — PartialScope
# ---------------------------------------------------------------------------

def test_partial_scope_date_window_where_clause() -> None:
    scope = PartialScope(
        date_column="created_at",
        date_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        date_end=datetime.datetime(2024, 1, 31, tzinfo=datetime.UTC),
    )
    where = scope.to_where_clause()
    assert "created_at" in where
    assert "2024-01-01" in where


def test_partial_scope_key_set_where_clause() -> None:
    scope = PartialScope(key_column="tenant_id", key_set=["acme", "globex"])
    where = scope.to_where_clause()
    assert "tenant_id" in where
    assert "acme" in where
    assert "globex" in where


def test_partial_scope_raw_predicate() -> None:
    scope = PartialScope(predicate="status = 'failed' AND retry_count > 3")
    where = scope.to_where_clause()
    assert "failed" in where
    assert "retry_count" in where
