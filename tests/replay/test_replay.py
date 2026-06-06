from __future__ import annotations

import pytest

from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.replay.manifest import ReplayManifest
from openneuronic.pipes.replay.point import ReplayPoint
from openneuronic.pipes.replay.snapshot import create_snapshot, _config_digest
from openneuronic.pipes.replay.store import InMemoryReplayStore
from openneuronic.pipes.runner import LocalRunner, RunResult
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource
from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# Stub source / sink
# ---------------------------------------------------------------------------


class _Src(AbstractSource):
    source_id = "stub.source"

    async def setup(self) -> None: pass

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        async def _gen():
            for i in range(5):
                yield Record(payload={"id": i})
        return _gen()

    async def teardown(self) -> None: pass


class _Snk(AbstractSink):
    _table = "stub.sink"

    async def setup(self) -> None: pass
    async def write(self, records): pass
    async def commit_bookmark(self, b): pass
    async def teardown(self) -> None: pass


# ---------------------------------------------------------------------------
# ReplayPoint
# ---------------------------------------------------------------------------


def test_replay_point_has_unique_id() -> None:
    p1 = ReplayPoint(pipe_id="p", source_bookmarks={}, schema_versions={}, contract_versions={}, config_digest="abc")
    p2 = ReplayPoint(pipe_id="p", source_bookmarks={}, schema_versions={}, contract_versions={}, config_digest="abc")
    assert p1.replay_id != p2.replay_id


def test_replay_point_created_at_is_utc() -> None:
    import datetime
    p = ReplayPoint(pipe_id="p", source_bookmarks={}, schema_versions={}, contract_versions={}, config_digest="x")
    assert p.created_at.tzinfo is not None


# ---------------------------------------------------------------------------
# create_snapshot
# ---------------------------------------------------------------------------


async def test_create_snapshot_captures_pipe_id() -> None:
    pipe = Pipe(id="orders-sync", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)
    assert result.replay_point is not None
    assert result.replay_point.pipe_id == "orders-sync"


async def test_create_snapshot_config_digest_is_stable() -> None:
    """The same pipe config always produces the same digest."""
    pipe_a = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    pipe_b = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    assert _config_digest(pipe_a) == _config_digest(pipe_b)


async def test_create_snapshot_digest_differs_on_different_mode() -> None:
    pipe_full = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    pipe_inc  = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.INCREMENTAL)
    assert _config_digest(pipe_full) != _config_digest(pipe_inc)


async def test_snapshot_includes_schema_version() -> None:
    from openneuronic.pipes.schema.base import Schema, schema_version
    from openneuronic.pipes.schema.field import Field
    from openneuronic.pipes.schema.field_type import FieldType

    @schema_version(3)
    class _S(Schema):
        id = Field(FieldType.INTEGER, nullable=False, primary_key=True)

    pipe = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL, schema=_S)
    result = await LocalRunner().run(pipe)
    assert result.replay_point.schema_versions.get("_S") == 3


async def test_snapshot_bookmark_value_stored() -> None:
    from openneuronic.pipes.core.bookmark import Bookmark
    from openneuronic.pipes.core.enums import BookmarkType

    pipe = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.INCREMENTAL)
    bm = Bookmark(pipe_id="p", column="updated_at", type=BookmarkType.INTEGER, value=42)
    result = await LocalRunner().run(pipe, bookmark=bm)
    assert result.replay_point.source_bookmarks.get("p") == 42


# ---------------------------------------------------------------------------
# InMemoryReplayStore
# ---------------------------------------------------------------------------


async def test_store_save_and_load() -> None:
    store = InMemoryReplayStore()
    point = ReplayPoint(
        pipe_id="p",
        source_bookmarks={"p": 99},
        schema_versions={"MySchema": 2},
        contract_versions={},
        config_digest="aabbccdd",
    )
    await store.save(point)
    loaded = await store.load(point.replay_id)
    assert loaded is not None
    assert loaded.pipe_id == "p"
    assert loaded.source_bookmarks == {"p": 99}


async def test_store_load_missing_returns_none() -> None:
    store = InMemoryReplayStore()
    assert await store.load("no-such-id") is None


async def test_store_list_for_pipe_returns_in_reverse_order() -> None:
    import datetime
    store = InMemoryReplayStore()
    older = ReplayPoint(
        pipe_id="p", source_bookmarks={}, schema_versions={}, contract_versions={},
        config_digest="x",
        created_at=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
    )
    newer = ReplayPoint(
        pipe_id="p", source_bookmarks={}, schema_versions={}, contract_versions={},
        config_digest="y",
        created_at=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
    )
    await store.save(older)
    await store.save(newer)
    results = await store.list_for_pipe("p")
    assert results[0].config_digest == "y"  # newest first
    assert results[1].config_digest == "x"


async def test_store_list_for_pipe_filters_by_pipe() -> None:
    store = InMemoryReplayStore()
    for pid in ("pipe-a", "pipe-b", "pipe-a"):
        await store.save(
            ReplayPoint(pipe_id=pid, source_bookmarks={}, schema_versions={}, contract_versions={}, config_digest="z")
        )
    assert len(await store.list_for_pipe("pipe-a")) == 2
    assert len(await store.list_for_pipe("pipe-b")) == 1


# ---------------------------------------------------------------------------
# ReplayManifest
# ---------------------------------------------------------------------------


def test_replay_manifest_default_run_id() -> None:
    m1 = ReplayManifest(
        replay_point_id="rp1",
        mode="BOOKMARK",
        scope={},
        source_snapshot={},
        sink_strategy="replace_full",
    )
    m2 = ReplayManifest(
        replay_point_id="rp1",
        mode="BOOKMARK",
        scope={},
        source_snapshot={},
        sink_strategy="replace_full",
    )
    assert m1.run_id != m2.run_id  # each manifest gets a unique run_id
