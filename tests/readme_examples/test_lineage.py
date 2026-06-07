"""README example tests — Lineage & Knowledge Graph section."""
from __future__ import annotations

import pathlib
import tempfile

import pytest

from openneuronic.pipes import (
    ColumnLineage,
    ColumnRef,
    DatasetRef,
    EdgeKind,
    JsonFileGraphStore,
    KnowledgeGraph,
    LineageTracker,
    NodeKind,
)


# ---------------------------------------------------------------------------
# Tests — LineageTracker
# ---------------------------------------------------------------------------

def test_lineage_tracker_graph_has_nodes_and_edges() -> None:
    tracker = LineageTracker()
    tracker.track_read("orders-sync",  DatasetRef("sqlserver.orders_raw"),  schema_version=1)
    tracker.track_write("orders-sync", DatasetRef("postgres.orders_clean"), schema_version=2, contract_version=1)

    graph = tracker.to_graph()
    node_ids = [n.id for n in graph.nodes]
    assert any("orders_raw"   in nid for nid in node_ids)
    assert any("orders_clean" in nid for nid in node_ids)


def test_lineage_tracker_graph_has_read_edge() -> None:
    tracker = LineageTracker()
    tracker.track_read("p", DatasetRef("src_ds"))
    graph = tracker.to_graph()
    reads = graph.edges_of_kind(EdgeKind.READS_FROM)
    assert len(reads) >= 1


def test_lineage_tracker_graph_has_write_edge() -> None:
    tracker = LineageTracker()
    tracker.track_write("p", DatasetRef("dst_ds"))
    graph = tracker.to_graph()
    writes = graph.edges_of_kind(EdgeKind.WRITES_TO)
    assert len(writes) >= 1


# ---------------------------------------------------------------------------
# Tests — KnowledgeGraph
# ---------------------------------------------------------------------------

def test_knowledge_graph_ensure_node_idempotent() -> None:
    g = KnowledgeGraph()
    g.ensure_node("pipe:orders-sync",   NodeKind.PIPE)
    g.ensure_node("dataset:orders_raw", NodeKind.DATASET)
    g.ensure_node("pipe:orders-sync",   NodeKind.PIPE)  # duplicate — should be idempotent
    nodes = list(g.nodes)
    pipe_nodes = [n for n in nodes if "orders-sync" in n.id]
    assert len(pipe_nodes) == 1


def test_knowledge_graph_add_edges_of_kind() -> None:
    g = KnowledgeGraph()
    g.ensure_node("pipe:orders-sync",     NodeKind.PIPE)
    g.ensure_node("dataset:orders_raw",   NodeKind.DATASET)
    g.ensure_node("dataset:orders_clean", NodeKind.DATASET)
    g.add_edge(EdgeKind.READS_FROM, "pipe:orders-sync", "dataset:orders_raw")
    g.add_edge(EdgeKind.WRITES_TO,  "pipe:orders-sync", "dataset:orders_clean")

    reads  = g.edges_of_kind(EdgeKind.READS_FROM)
    writes = g.edges_of_kind(EdgeKind.WRITES_TO)
    assert len(reads)  == 1
    assert len(writes) == 1


def test_knowledge_graph_get_node_returns_node() -> None:
    g = KnowledgeGraph()
    g.ensure_node("pipe:x", NodeKind.PIPE)
    node = g.get_node("pipe:x")
    assert node is not None
    assert node.id == "pipe:x"


def test_knowledge_graph_get_node_returns_none_for_missing() -> None:
    g = KnowledgeGraph()
    assert g.get_node("nonexistent") is None


# ---------------------------------------------------------------------------
# Tests — JsonFileGraphStore
# ---------------------------------------------------------------------------

async def test_json_file_graph_store_save_and_load() -> None:
    g = KnowledgeGraph()
    g.ensure_node("pipe:test-save", NodeKind.PIPE)
    g.ensure_node("dataset:ds-a",   NodeKind.DATASET)
    g.add_edge(EdgeKind.READS_FROM, "pipe:test-save", "dataset:ds-a")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = JsonFileGraphStore(str(pathlib.Path(tmpdir) / "lineage.json"))
        await store.save(g)

        loaded = await store.load()
        assert loaded.get_node("pipe:test-save") is not None
        assert loaded.get_node("dataset:ds-a") is not None
        reads = loaded.edges_of_kind(EdgeKind.READS_FROM)
        assert len(reads) >= 1


async def test_json_file_graph_store_merge_event() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(pathlib.Path(tmpdir) / "lineage.json")
        store = JsonFileGraphStore(path)

        g1 = KnowledgeGraph()
        g1.ensure_node("pipe:p1", NodeKind.PIPE)
        await store.save(g1)

        tracker2 = LineageTracker()
        tracker2.track_read("p2", DatasetRef("ds-merge"))
        await store.merge_event(tracker2.to_graph())

        merged = await store.load()
        node_ids = [n.id for n in merged.nodes]
        assert any("p1" in nid for nid in node_ids)
        assert any("ds-merge" in nid for nid in node_ids)


# ---------------------------------------------------------------------------
# Tests — ColumnLineage
# ---------------------------------------------------------------------------

def test_column_lineage_constructs() -> None:
    source_ds = DatasetRef(name="sqlserver.orders_raw",  schema_name="OrderSchemaV1", schema_version=1)
    target_ds = DatasetRef(name="postgres.orders_clean", schema_name="OrderSchemaV2", schema_version=2)

    lineage = ColumnLineage(
        output=ColumnRef(dataset=target_ds, column_name="total_amount_eur"),
        inputs=[
            ColumnRef(dataset=source_ds, column_name="amount"),
            ColumnRef(dataset=source_ds, column_name="currency"),
        ],
        expression="amount * eur_rate",
        transform_kind="derive",
    )

    assert lineage.output.column_name == "total_amount_eur"
    assert len(lineage.inputs) == 2
    assert lineage.expression == "amount * eur_rate"
