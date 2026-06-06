from __future__ import annotations

import copy
from collections.abc import AsyncIterator
from typing import Any

import pytest

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import BookmarkType, CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.runner import LocalRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class StubSource(AbstractSource):
    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self._payloads = payloads
        self.setup_called = False
        self.teardown_called = False

    async def setup(self) -> None:
        self.setup_called = True

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            for payload in self._payloads:
                yield Record(payload=copy.deepcopy(payload))

        return _gen()

    async def teardown(self) -> None:
        self.teardown_called = True


class FailingSource(AbstractSource):
    async def setup(self) -> None:
        pass

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            yield Record(payload={"id": 1})
            raise RuntimeError("Source read failure")

        return _gen()

    async def teardown(self) -> None:
        pass


class StubSink(AbstractSink):
    def __init__(self, fail_on_write: bool = False) -> None:
        self.written: list[list[Record]] = []
        self.committed_bookmarks: list[Bookmark] = []
        self.fail_on_write = fail_on_write
        self.setup_called = False
        self.teardown_called = False

    async def setup(self) -> None:
        self.setup_called = True

    async def write(self, records: list[Record]) -> None:
        if self.fail_on_write:
            raise RuntimeError("Simulated write failure")
        if records:
            self.written.append(records)

    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        self.committed_bookmarks.append(copy.deepcopy(bookmark))

    async def teardown(self) -> None:
        self.teardown_called = True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_runner_reads_and_writes_all_records() -> None:
    source = StubSource([{"id": 1}, {"id": 2}, {"id": 3}])
    sink = StubSink()
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    result = await LocalRunner(batch_size=10).run(pipe)

    assert result.success
    assert result.records_read == 3
    assert result.records_written == 3
    assert result.records_dropped == 0


async def test_runner_calls_lifecycle_hooks() -> None:
    source = StubSource([{"id": 1}])
    sink = StubSink()
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    await LocalRunner().run(pipe)

    assert source.setup_called
    assert source.teardown_called
    assert sink.setup_called
    assert sink.teardown_called


async def test_runner_batches_correctly() -> None:
    payloads = [{"id": i} for i in range(7)]
    source = StubSource(payloads)
    sink = StubSink()
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    result = await LocalRunner(batch_size=3).run(pipe)

    assert result.records_written == 7
    # 7 records / batch_size 3 → batches of [3, 3, 1]
    assert len(sink.written) == 3
    assert [len(b) for b in sink.written] == [3, 3, 1]


async def test_bookmark_advanced_after_successful_write() -> None:
    source = StubSource(
        [{"id": 1, "ts": "2024-01-02"}, {"id": 2, "ts": "2024-01-03"}]
    )
    sink = StubSink()
    bookmark = Bookmark(
        pipe_id="p",
        column="ts",
        type=BookmarkType.DATETIME,
        value="2024-01-01",
    )
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.INCREMENTAL)

    result = await LocalRunner(batch_size=10).run(pipe, bookmark=bookmark)

    assert result.success
    assert len(sink.committed_bookmarks) == 1
    assert sink.committed_bookmarks[0].value == "2024-01-03"
    assert sink.committed_bookmarks[0].previous_value == "2024-01-01"
    # In-memory bookmark object is also updated.
    assert bookmark.value == "2024-01-03"
    assert bookmark.previous_value == "2024-01-01"
    assert bookmark.batch_count == 1


async def test_bookmark_not_committed_when_write_fails() -> None:
    source = StubSource([{"id": 1, "ts": "2024-01-02"}])
    sink = StubSink(fail_on_write=True)
    bookmark = Bookmark(
        pipe_id="p",
        column="ts",
        type=BookmarkType.DATETIME,
        value="2024-01-01",
    )
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.INCREMENTAL)

    with pytest.raises(RuntimeError, match="Simulated write failure"):
        await LocalRunner(batch_size=10).run(pipe, bookmark=bookmark)

    assert len(sink.committed_bookmarks) == 0
    # Original bookmark value unchanged.
    assert bookmark.value == "2024-01-01"


async def test_no_bookmark_committed_for_full_mode() -> None:
    source = StubSource([{"id": 1, "ts": "2024-01-02"}])
    sink = StubSink()
    bookmark = Bookmark(
        pipe_id="p",
        column="ts",
        type=BookmarkType.DATETIME,
        value="2024-01-01",
    )
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    await LocalRunner(batch_size=10).run(pipe, bookmark=bookmark)

    assert len(sink.committed_bookmarks) == 0


async def test_teardown_called_even_on_source_failure() -> None:
    source = FailingSource()
    sink = StubSink()
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    with pytest.raises(RuntimeError, match="Source read failure"):
        await LocalRunner(batch_size=10).run(pipe)

    assert sink.teardown_called


async def test_run_result_records_error() -> None:
    source = StubSource([{"id": 1}])
    sink = StubSink(fail_on_write=True)
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    with pytest.raises(RuntimeError):
        result_ref: list[Any] = []
        try:
            await LocalRunner(batch_size=10).run(pipe)
        except RuntimeError as exc:
            result_ref.append(exc)
            raise

    assert len(result_ref) == 1


async def test_empty_source_produces_zero_records() -> None:
    source = StubSource([])
    sink = StubSink()
    pipe = Pipe(id="p", source=source, sink=sink, mode=CopyMode.FULL)

    result = await LocalRunner().run(pipe)

    assert result.success
    assert result.records_read == 0
    assert result.records_written == 0
    assert sink.written == []
