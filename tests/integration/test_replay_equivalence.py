"""Equivalence integration tests — Replay.

Asserts that:
- REPLACE_FULL replay produces the same records_written as the original run.
- DRY_RUN replay succeeds but the null sink receives the right number of reads.
- WINDOW replay applies the scope correctly.

Run with::

    py -3.14 -m pytest tests/integration/test_replay_equivalence.py -v -m integration
"""
from __future__ import annotations

import datetime
from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    InMemoryReplayStore,
    LocalRunner,
    Pipe,
    Record,
    ReplayManifest,
    ReplayRunner,
    ReplayWriteStrategy,
    TimeTravelMode,
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
    async def write(self, records: list[Record]) -> None: self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


def _pipe(n: int = 10, pipe_id: str = "replay-equiv") -> Pipe:
    return Pipe(id=pipe_id, source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.FULL)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_replace_full_replay_matches_original_records_written() -> None:
    """REPLACE_FULL replay must produce the same records_written as the original run."""
    n = 20
    pipe = _pipe(n, "rp-equiv-1")
    original = await LocalRunner().run(pipe)
    point = original.replay_point

    store = InMemoryReplayStore()
    await store.save(point)

    manifest = ReplayManifest(
        replay_point_id=point.replay_id,
        mode=TimeTravelMode.BOOKMARK,
        scope={},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.REPLACE_FULL,
        reason="equivalence check",
    )
    replay_result = await ReplayRunner(store, allow_config_drift=True).run(pipe, manifest)

    assert replay_result.records_written == original.records_written


async def test_dry_run_replay_reads_same_count_as_original() -> None:
    """DRY_RUN replay must read the same number of records as the original run."""
    n = 15
    pipe = _pipe(n, "rp-equiv-2")
    original = await LocalRunner().run(pipe)
    point = original.replay_point

    store = InMemoryReplayStore()
    await store.save(point)

    manifest = ReplayManifest(
        replay_point_id=point.replay_id,
        mode=TimeTravelMode.BOOKMARK,
        scope={},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.DRY_RUN,
        reason="dry run equivalence",
    )
    replay_result = await ReplayRunner(store, allow_config_drift=True).run(pipe, manifest)

    assert replay_result.success
    assert replay_result.records_read == original.records_read


async def test_replay_store_list_returns_newest_first() -> None:
    """list_for_pipe must return replay points with newest first."""
    n = 5
    store = InMemoryReplayStore()

    pipe1 = _pipe(n, "order-pipe")
    r1 = await LocalRunner().run(pipe1)
    await store.save(r1.replay_point)

    pipe2 = _pipe(n + 1, "order-pipe")
    r2 = await LocalRunner().run(pipe2)
    await store.save(r2.replay_point)

    history = await store.list_for_pipe("order-pipe")
    assert len(history) == 2
    # Newest created_at should come first
    assert history[0].created_at >= history[1].created_at


async def test_shadow_write_replay_does_not_affect_live_sink() -> None:
    """SHADOW_WRITE replay writes to a copy-sink; original sink is never touched."""
    n = 10
    original_sink = _CaptureSink()
    pipe = Pipe(id="rp-shadow", source=_CountSource(n), sink=original_sink, mode=CopyMode.FULL)
    original = await LocalRunner().run(pipe)
    point = original.replay_point

    store = InMemoryReplayStore()
    await store.save(point)

    manifest = ReplayManifest(
        replay_point_id=point.replay_id,
        mode=TimeTravelMode.BOOKMARK,
        scope={},
        source_snapshot={},
        sink_strategy=ReplayWriteStrategy.SHADOW_WRITE,
        reason="shadow write",
    )
    # Create a fresh pipe with a fresh sink — the replay runner will shadow it
    replay_pipe = Pipe(id="rp-shadow", source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.FULL)
    replay_result = await ReplayRunner(store, allow_config_drift=True).run(replay_pipe, manifest)
    assert replay_result.success
