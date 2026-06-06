from __future__ import annotations

import pytest

from openneuronic.pipes.lineage.events import LineageEventKind
from openneuronic.pipes.lineage.graph.model import EdgeKind, KnowledgeGraph, NodeKind
from openneuronic.pipes.lineage.refs import ColumnRef, DatasetRef
from openneuronic.pipes.lineage.tracker import LineageTracker


# ---------------------------------------------------------------------------
# DatasetRef / ColumnRef
# ---------------------------------------------------------------------------


def test_dataset_ref_equality() -> None:
    a = DatasetRef("sqlserver.now.Orders", "OrderSchema", 2)
    b = DatasetRef("sqlserver.now.Orders", "OrderSchema", 2)
    assert a == b


def test_column_ref_equality() -> None:
    ds = DatasetRef("ds")
    a = ColumnRef(ds, "total_amount")
    b = ColumnRef(ds, "total_amount")
    assert a == b


# ---------------------------------------------------------------------------
# LineageTracker
# ---------------------------------------------------------------------------


def test_tracker_generates_run_id() -> None:
    t = LineageTracker()
    assert t.run_id and len(t.run_id) == 36  # UUID format


def test_tracker_custom_run_id() -> None:
    t = LineageTracker(run_id="my-run")
    assert t.run_id == "my-run"


def test_tracker_track_read_adds_event() -> None:
    t = LineageTracker(run_id="r1")
    t.track_read("pipe-a", DatasetRef("db.orders"))
    assert len(t.events) == 1
    assert t.events[0].kind == LineageEventKind.READ
    assert t.events[0].source_ref == DatasetRef("db.orders")


def test_tracker_track_write_adds_event() -> None:
    t = LineageTracker(run_id="r1")
    t.track_write("pipe-a", DatasetRef("db.orders_clean"), contract_version=2)
    e = t.events[0]
    assert e.kind == LineageEventKind.WRITE
    assert e.target_ref == DatasetRef("db.orders_clean")
    assert e.contract_version == 2


def test_tracker_events_for_run_filters_correctly() -> None:
    t = LineageTracker(run_id="r1")
    t.track_read("p", DatasetRef("a"))
    # Manually emit a second event with a different run_id
    from openneuronic.pipes.lineage.events import LineageEvent
    t.emit(
        LineageEvent(
            kind=LineageEventKind.READ,
            pipe_id="p",
            run_id="other-run",
            source_ref=DatasetRef("b"),
        )
    )
    assert len(t.events_for_run("r1")) == 1
    assert len(t.events_for_run("other-run")) == 1


def test_tracker_to_graph_produces_nodes_and_edges() -> None:
    t = LineageTracker(run_id="r1")
    t.track_read("pipe-x", DatasetRef("src.orders"))
    t.track_write("pipe-x", DatasetRef("dst.orders_clean"))

    graph = t.to_graph()
    kinds = {n.kind for n in graph.nodes}
    assert NodeKind.PIPE in kinds
    assert NodeKind.RUN in kinds
    assert NodeKind.DATASET in kinds

    edge_kinds = {e.kind for e in graph.edges}
    assert EdgeKind.READS_FROM in edge_kinds
    assert EdgeKind.WRITES_TO in edge_kinds
    assert EdgeKind.EXECUTED_IN in edge_kinds


# ---------------------------------------------------------------------------
# KnowledgeGraph
# ---------------------------------------------------------------------------


def test_graph_deduplicates_nodes() -> None:
    g = KnowledgeGraph()
    from openneuronic.pipes.lineage.graph.model import Node
    g.add_node(Node(id="pipe:a", kind=NodeKind.PIPE))
    g.add_node(Node(id="pipe:a", kind=NodeKind.PIPE))  # duplicate
    assert len(g.nodes) == 1


def test_graph_edges_from() -> None:
    g = KnowledgeGraph()
    from openneuronic.pipes.lineage.graph.model import Node
    g.add_node(Node("n1", NodeKind.PIPE))
    g.add_node(Node("n2", NodeKind.DATASET))
    g.add_edge(EdgeKind.READS_FROM, "n1", "n2")
    assert len(g.edges_from("n1")) == 1
    assert len(g.edges_to("n2")) == 1
    assert g.edges_from("n2") == []


def test_graph_edges_of_kind() -> None:
    g = KnowledgeGraph()
    from openneuronic.pipes.lineage.graph.model import Node
    g.add_node(Node("p", NodeKind.PIPE))
    g.add_node(Node("d1", NodeKind.DATASET))
    g.add_node(Node("d2", NodeKind.DATASET))
    g.add_edge(EdgeKind.READS_FROM, "p", "d1")
    g.add_edge(EdgeKind.WRITES_TO, "p", "d2")
    assert len(g.edges_of_kind(EdgeKind.READS_FROM)) == 1
    assert len(g.edges_of_kind(EdgeKind.WRITES_TO)) == 1


# ---------------------------------------------------------------------------
# LocalRunner produces lineage on RunResult
# ---------------------------------------------------------------------------


from collections.abc import AsyncIterator

from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.runner import LocalRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource
from openneuronic.pipes.core.bookmark import Bookmark


class _Src(AbstractSource):
    source_id = "test.source"

    async def setup(self) -> None: pass

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        async def _gen():
            yield Record(payload={"id": 1})
        return _gen()

    async def teardown(self) -> None: pass


class _Snk(AbstractSink):
    _table = "test.sink"

    async def setup(self) -> None: pass
    async def write(self, records): pass
    async def commit_bookmark(self, b): pass
    async def teardown(self) -> None: pass


async def test_runner_attaches_lineage_to_result() -> None:
    pipe = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)
    assert result.lineage is not None
    assert len(result.lineage) >= 2  # at least READ + WRITE
    kinds = {e.kind for e in result.lineage}
    assert LineageEventKind.READ in kinds
    assert LineageEventKind.WRITE in kinds


async def test_runner_attaches_replay_point_on_success() -> None:
    pipe = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)
    assert result.replay_point is not None
    assert result.replay_point.pipe_id == "p"
    assert result.replay_point.config_digest  # non-empty


async def test_runner_run_id_in_lineage_events() -> None:
    pipe = Pipe(id="p", source=_Src(), sink=_Snk(), mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)
    for event in result.lineage:
        assert event.run_id  # non-empty UUID string
