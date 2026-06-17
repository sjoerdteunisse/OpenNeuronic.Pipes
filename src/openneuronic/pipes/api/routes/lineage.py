"""Lineage endpoints: full graph and per-pipe subgraph."""
from __future__ import annotations

from typing import Any

from flask import Blueprint, current_app, jsonify

from openneuronic.pipes.api.serializers import knowledge_graph_to_dict
from openneuronic.pipes.lineage.graph.model import EdgeKind, KnowledgeGraph

lineage_bp = Blueprint("lineage", __name__, url_prefix="/lineage")


def _state() -> dict[str, Any]:
    return current_app.extensions["onpipes"]


@lineage_bp.get("/")
def get_full_graph() -> tuple:
    graph: KnowledgeGraph = _state()["lineage_graph"]
    return jsonify(knowledge_graph_to_dict(graph)), 200


@lineage_bp.get("/pipes/<pipe_id>")
def get_pipe_lineage(pipe_id: str) -> tuple:
    state = _state()
    registry = state["registry"]
    if registry.pipes.get(pipe_id) is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404

    full: KnowledgeGraph = state["lineage_graph"]
    node_id = f"pipe:{pipe_id}"

    # Collect the pipe node and all directly connected nodes/edges.
    relevant_node_ids: set[str] = {node_id}
    relevant_edges = []
    for edge in full._edges:
        if edge.source_id == node_id or edge.target_id == node_id:
            relevant_edges.append(edge)
            relevant_node_ids.add(edge.source_id)
            relevant_node_ids.add(edge.target_id)

    nodes = [
        {"id": n.id, "kind": str(n.kind), "properties": n.properties}
        for nid, n in full._nodes.items()
        if nid in relevant_node_ids
    ]
    edges = [
        {
            "id": e.id,
            "kind": str(e.kind),
            "source_id": e.source_id,
            "target_id": e.target_id,
            "properties": e.properties,
        }
        for e in relevant_edges
    ]
    return jsonify({"nodes": nodes, "edges": edges}), 200
