"""Equivalence integration tests — Copy Modes.

Asserts that:
- FULL mode and INCREMENTAL mode (when reading all records) produce the same final row count.
- PARTIAL mode deletes the exact scope before writing.
- RunResult.records_written matches the actual records captured by the sink.

Run with::

    py -3.14 -m pytest tests/integration/test_copy_modes_equivalence.py -v -m integration
"""
from __future__ import annotations

import datetime
from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    LocalRunner,
    PartialScope,
    Pipe,
    Record,
)
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int) -> None:
        self._n = n

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _g():
            for i in range(n):
                yield Record(payload={
                    "id": i,
                    "created_at": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                })
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self.deleted_scope: PartialScope | None = None
        self.prepared = False
        self.committed = False
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass
    async def write(self, records: list[Record]) -> None: self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: self.prepared = True
    async def commit_full_load(self) -> None: self.committed = True
    async def delete_scope(self, scope: PartialScope) -> None: self.deleted_scope = scope


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_full_and_incremental_write_same_count() -> None:
    """FULL and INCREMENTAL modes both read all source records → same written count."""
    n = 40

    sink_full = _CaptureSink()
    pipe_full = Pipe(id="full-mode", source=_CountSource(n), sink=sink_full, mode=CopyMode.FULL)

    sink_inc = _CaptureSink()
    pipe_inc = Pipe(id="inc-mode", source=_CountSource(n), sink=sink_inc, mode=CopyMode.INCREMENTAL)

    result_full = await LocalRunner().run(pipe_full)
    result_inc  = await LocalRunner().run(pipe_inc)

    assert result_full.records_written == result_inc.records_written == n


async def test_full_mode_staging_lifecycle_matches_written_count() -> None:
    """FULL mode calls prepare→write→commit; records in sink == RunResult.records_written."""
    n = 25
    sink = _CaptureSink()
    pipe = Pipe(id="full-lifecycle", source=_CountSource(n), sink=sink, mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)

    assert sink.prepared
    assert sink.committed
    assert len(sink.written) == result.records_written == n


async def test_partial_mode_deletes_scope_then_writes() -> None:
    """PARTIAL mode calls delete_scope before writing; written count == n."""
    n = 15
    scope = PartialScope(
        date_column="created_at",
        date_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        date_end=datetime.datetime(2024, 1, 31, tzinfo=datetime.UTC),
    )
    sink = _CaptureSink()
    pipe = Pipe(id="partial-mode", source=_CountSource(n), sink=sink, mode=CopyMode.PARTIAL, scope=scope)
    result = await LocalRunner().run(pipe)

    assert sink.deleted_scope is scope
    assert result.records_written == n
    assert len(sink.written) == n


async def test_runresult_records_written_matches_sink_capture() -> None:
    """RunResult.records_written must always equal the number of records in the sink."""
    for mode in (CopyMode.INCREMENTAL, CopyMode.FULL):
        n = 10
        sink = _CaptureSink()
        pipe = Pipe(id=f"count-check-{mode.value}", source=_CountSource(n), sink=sink, mode=mode)
        result = await LocalRunner().run(pipe)
        assert result.records_written == len(sink.written), (
            f"mode={mode}: RunResult says {result.records_written} but sink captured {len(sink.written)}"
        )


async def test_records_read_matches_source_count() -> None:
    """RunResult.records_read must equal the source record count."""
    n = 33
    sink = _CaptureSink()
    pipe = Pipe(id="read-count", source=_CountSource(n), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe)
    assert result.records_read == n
