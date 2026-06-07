"""Equivalence integration tests — Bookmarks.

Asserts that:
- InMemoryBookmarkStore save→reload preserves the exact value.
- A second incremental run with an already-advanced bookmark writes 0 records
  when the source has no new data past the bookmark.

Run with::

    py -3.14 -m pytest tests/integration/test_bookmark_equivalence.py -v -m integration
"""
from __future__ import annotations

import datetime
from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    Bookmark,
    BookmarkType,
    CopyMode,
    InMemoryBookmarkStore,
    LocalRunner,
    Pipe,
    Record,
)
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _SequencedSource(AbstractSource):
    """Emits records with ids 0..n-1; respects an INTEGER bookmark to skip old records."""

    source_id = "seq-source"

    def __init__(self, n: int) -> None:
        self._n = n

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        n = self._n
        min_id = 0
        if bookmark is not None and bookmark.type == BookmarkType.INTEGER:
            min_id = int(bookmark.value) + 1

        async def _g():
            for i in range(n):
                if i >= min_id:
                    yield Record(payload={"id": i, "seq": i})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass
    async def write(self, records: list[Record]) -> None: self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_bookmark_save_reload_preserves_integer_value() -> None:
    """Saving and loading an INTEGER bookmark round-trips the value exactly."""
    store = InMemoryBookmarkStore()
    bm = Bookmark(pipe_id="bm-equiv", column="seq", type=BookmarkType.INTEGER, value=9999)
    await store.save(bm)

    loaded = await store.load("bm-equiv")
    assert loaded is not None
    assert loaded.value == 9999
    assert loaded.column == "seq"
    assert loaded.type == BookmarkType.INTEGER


async def test_bookmark_save_reload_preserves_datetime_value() -> None:
    """Saving and loading a DATETIME bookmark round-trips the value exactly."""
    store = InMemoryBookmarkStore()
    dt = datetime.datetime(2024, 6, 15, 12, 30, 0, tzinfo=datetime.UTC)
    bm = Bookmark(pipe_id="bm-dt", column="updated_at", type=BookmarkType.DATETIME, value=dt)
    await store.save(bm)

    loaded = await store.load("bm-dt")
    assert loaded is not None
    assert loaded.value == dt


async def test_incremental_second_run_skips_old_records() -> None:
    """When the bookmark is advanced past all records, the second incremental run writes 0."""
    n = 10  # records with ids 0..9

    sink1 = _CaptureSink()
    pipe1 = Pipe(id="inc-bm-equiv", source=_SequencedSource(n), sink=sink1, mode=CopyMode.INCREMENTAL)

    # First run: no bookmark — reads all n records
    result1 = await LocalRunner().run(pipe1)
    assert result1.records_written == n

    # Advance bookmark to the last id so next run starts after all known records
    bookmark = Bookmark(pipe_id="inc-bm-equiv", column="seq", type=BookmarkType.INTEGER, value=n - 1)

    sink2 = _CaptureSink()
    pipe2 = Pipe(id="inc-bm-equiv", source=_SequencedSource(n), sink=sink2, mode=CopyMode.INCREMENTAL)

    # Second run: bookmark already at max → source emits 0 records
    result2 = await LocalRunner().run(pipe2, bookmark=bookmark)
    assert result2.records_written == 0


async def test_incremental_partial_run_reads_only_new_records() -> None:
    """Bookmark set to midpoint causes second run to read only the new half."""
    n = 20
    mid = 9  # bookmark value = id 9; second run should read ids 10..19

    bookmark = Bookmark(pipe_id="mid-bm", column="seq", type=BookmarkType.INTEGER, value=mid)

    sink = _CaptureSink()
    pipe = Pipe(id="mid-bm", source=_SequencedSource(n), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe, bookmark=bookmark)

    expected = n - (mid + 1)  # 10 records
    assert result.records_written == expected
    assert result.records_read == expected
