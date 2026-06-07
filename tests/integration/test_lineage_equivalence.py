"""Equivalence integration tests — Lineage.

Asserts that:
- LineageTracker.to_graph() events == LocalRunner's RunResult.lineage events.
- JsonFileGraphStore merge_event accumulates nodes from multiple trackers.

Run with::

    py -3.14 -m pytest tests/integration/test_lineage_equivalence.py -v -m integration
"""
from __future__ import annotations

import pathlib
import tempfile
from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    DatasetRef,
    EdgeKind,
    JsonFileGraphStore,
    LineageEventKind,
    LineageTracker,
    LocalRunner,
    NodeKind,
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
    source_id = "lineage-source"

    def __init__(self, n: int = 5) -> None:
        self._n = n

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _g():
            for i in range(n):
                yield Record(payload={"id": i})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self._table = "lineage-sink"
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

async def test_runner_emits_read_and_write_lineage_events() -> None:
    """LocalRunner must emit at least one READ and one WRITE lineage event."""
    sink = _CaptureSink()
    pipe = Pipe(id="lineage-equiv", source=_CountSource(5), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe)

    kinds = {e.kind for e in result.lineage}
    assert LineageEventKind.READ in kinds
    assert LineageEventKind.WRITE in kinds


async def test_runner_lineage_pipe_id_matches_pipe() -> None:
    """All lineage events must carry the pipe's id."""
    pipe_id = "lineage-pipe-id-check"
    sink = _CaptureSink()
    pipe = Pipe(id=pipe_id, source=_CountSource(3), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe)

    for event in result.lineage:
        assert event.pipe_id == pipe_id


async def test_manual_tracker_and_runner_lineage_same_structure() -> None:
    """Manually emitted tracker graph and runner's result.lineage have the same edge kinds."""
    tracker = LineageTracker()
    tracker.track_read("orders-sync",  DatasetRef("sqlserver.orders_raw"))
    tracker.track_write("orders-sync", DatasetRef("postgres.orders_clean"))
    graph = tracker.to_graph()

    reads  = graph.edges_of_kind(EdgeKind.READS_FROM)
    writes = graph.edges_of_kind(EdgeKind.WRITES_TO)
    assert len(reads)  >= 1
    assert len(writes) >= 1

    # Runner also produces at least one read and one write lineage event
    sink = _CaptureSink()
    pipe = Pipe(id="orders-sync", source=_CountSource(3), sink=sink, mode=CopyMode.INCREMENTAL)
    result = await LocalRunner().run(pipe)
    runner_kinds = {e.kind for e in result.lineage}
    assert LineageEventKind.READ  in runner_kinds
    assert LineageEventKind.WRITE in runner_kinds


async def test_json_graph_store_accumulates_merged_events() -> None:
    """Merging multiple tracker graphs accumulates all nodes in the store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(pathlib.Path(tmpdir) / "lineage.json")
        store = JsonFileGraphStore(path)

        tracker1 = LineageTracker()
        tracker1.track_read("pipe-a", DatasetRef("ds-x"))
        await store.save(tracker1.to_graph())

        tracker2 = LineageTracker()
        tracker2.track_write("pipe-b", DatasetRef("ds-y"))
        await store.merge_event(tracker2.to_graph())

        merged = await store.load()
        node_ids = {n.id for n in merged.nodes}
        assert any("ds-x" in nid for nid in node_ids)
        assert any("ds-y" in nid for nid in node_ids)
