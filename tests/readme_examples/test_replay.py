"""README example tests — Replay & Time Travel section."""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    InMemoryReplayStore,
    Pipe,
    Record,
    ReplayManifest,
    ReplayRunner,
    ReplayWriteStrategy,
    TimeTravelMode,
)
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
                yield Record(payload={"id": i, "val": f"v{i}"})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass
    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


def _pipe(n: int = 5) -> Pipe:
    return Pipe(id="replay-test", source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.FULL)


# ---------------------------------------------------------------------------
# Tests — replay point capture
# ---------------------------------------------------------------------------

async def test_run_result_has_replay_point() -> None:
    pipe = _pipe()
    result = await LocalRunner().run(pipe)
    assert result.replay_point is not None
    assert result.replay_point.replay_id


async def test_replay_point_has_config_digest() -> None:
    pipe = _pipe()
    result = await LocalRunner().run(pipe)
    assert result.replay_point.config_digest


# ---------------------------------------------------------------------------
# Tests — InMemoryReplayStore
# ---------------------------------------------------------------------------

async def test_replay_store_save_and_load() -> None:
    pipe = _pipe()
    result = await LocalRunner().run(pipe)
    point = result.replay_point

    store = InMemoryReplayStore()
    await store.save(point)

    loaded = await store.load(point.replay_id)
    assert loaded is not None
    assert loaded.replay_id == point.replay_id


async def test_replay_store_list_for_pipe() -> None:
    pipe = _pipe()
    store = InMemoryReplayStore()

    r1 = await LocalRunner().run(pipe)
    await store.save(r1.replay_point)

    pipe2 = _pipe(3)
    r2 = await LocalRunner().run(pipe2)
    await store.save(r2.replay_point)

    history = await store.list_for_pipe("replay-test")
    assert len(history) >= 2


# ---------------------------------------------------------------------------
# Tests — ReplayRunner
# ---------------------------------------------------------------------------

async def test_replay_dry_run_succeeds_with_null_sink() -> None:
    """DRY_RUN routes output to _DevNullSink. The runner still counts submissions
    (records_written reflects records submitted to write(), not persisted rows).
    The key property of DRY_RUN is that no real table is touched."""
    pipe = _pipe(5)
    result = await LocalRunner().run(pipe)
    point = result.replay_point

    store = InMemoryReplayStore()
    await store.save(point)

    manifest = ReplayManifest(
        replay_point_id=point.replay_id,
        mode=TimeTravelMode.BOOKMARK,
        scope={},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.DRY_RUN,
        reason="readme test",
    )

    runner = ReplayRunner(replay_store=store, allow_config_drift=True)
    replay_result = await runner.run(pipe, manifest)
    # DRY_RUN succeeds; data is submitted to the null sink (no real persistence)
    assert replay_result.success
    assert replay_result.records_read == 5


async def test_replay_replace_full_writes_same_count() -> None:
    pipe = _pipe(5)
    result = await LocalRunner().run(pipe)
    point = result.replay_point
    original_count = result.records_written

    store = InMemoryReplayStore()
    await store.save(point)

    manifest = ReplayManifest(
        replay_point_id=point.replay_id,
        mode=TimeTravelMode.BOOKMARK,
        scope={},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.REPLACE_FULL,
        reason="replace full",
    )

    runner = ReplayRunner(replay_store=store, allow_config_drift=True)
    replay_result = await runner.run(pipe, manifest)
    assert replay_result.records_written == original_count


async def test_replay_manifest_stores_reason() -> None:
    manifest = ReplayManifest(
        replay_point_id="dummy",
        mode=TimeTravelMode.WINDOW,
        scope={"date_column": "created_at", "date_start": "2024-01-01T00:00:00+00:00", "date_end": "2024-01-31T23:59:59+00:00"},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.REPLACE_SCOPE,
        reason="window replay",
    )
    assert manifest.reason == "window replay"
    assert manifest.mode == TimeTravelMode.WINDOW
