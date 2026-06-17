"""Shared test helpers and fixtures for the API tests."""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from openneuronic.pipes.api import Registry, create_app
from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import BookmarkType, CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Minimal test doubles
# ---------------------------------------------------------------------------


class MemorySource(AbstractSource):
    """Yields a fixed list of records."""

    def __init__(self, payloads: list[dict[str, Any]] | None = None) -> None:
        self._payloads = payloads or [{"id": i} for i in range(1, 4)]

    async def setup(self) -> None:
        pass

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            for p in self._payloads:
                yield Record(payload=p)

        return _gen()

    async def teardown(self) -> None:
        pass


class MemorySink(AbstractSink):
    """Accumulates written records in memory."""

    def __init__(self) -> None:
        self.batches: list[list[Record]] = []

    async def setup(self) -> None:
        pass

    async def write(self, records: list[Record]) -> None:
        self.batches.append(list(records))

    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        pass

    async def teardown(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pipe() -> Pipe:
    return Pipe(
        id="test-pipe",
        source=MemorySource(),
        sink=MemorySink(),
        mode=CopyMode.INCREMENTAL,
    )


@pytest.fixture()
def opus(pipe: Pipe) -> Opus:
    o = Opus(id="test-opus", durable=False)
    o.add_segment(PipeSegment(id="step-1", pipe=pipe))
    return o


@pytest.fixture()
def registry(pipe: Pipe, opus: Opus) -> Registry:
    reg = Registry()
    reg.pipes.register(pipe)
    reg.opus.register(opus)
    return reg


@pytest.fixture()
def client(registry: Registry):
    app = create_app(registry, config={"TESTING": True})
    with app.test_client() as c:
        yield c
