"""KnowledgeGraph persistence — JSON file store and Redis store."""
from __future__ import annotations

import json
import pathlib
from abc import ABC, abstractmethod
from typing import Any

from openneuronic.pipes.lineage.graph.model import Edge, EdgeKind, KnowledgeGraph, Node, NodeKind


class GraphStore(ABC):
    """Abstract persistence layer for a :class:`~openneuronic.pipes.lineage.graph.model.KnowledgeGraph`."""

    @abstractmethod
    async def save(self, graph: KnowledgeGraph) -> None:
        """Persist the full graph, merging with any existing stored state."""

    @abstractmethod
    async def load(self) -> KnowledgeGraph:
        """Load and return the stored graph (empty graph when nothing persisted)."""

    @abstractmethod
    async def merge_event(self, graph: KnowledgeGraph) -> None:
        """Merge new nodes/edges into the persisted graph without a full reload."""


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _graph_to_dict(graph: KnowledgeGraph) -> dict[str, Any]:
    return {
        "nodes": [
            {"id": n.id, "kind": str(n.kind), "properties": n.properties}
            for n in graph.nodes
        ],
        "edges": [
            {
                "id": e.id,
                "kind": str(e.kind),
                "source_id": e.source_id,
                "target_id": e.target_id,
                "properties": e.properties,
            }
            for e in graph.edges
        ],
    }


def _dict_to_graph(d: dict[str, Any]) -> KnowledgeGraph:
    g = KnowledgeGraph()
    for nd in d.get("nodes", []):
        g.add_node(Node(id=nd["id"], kind=NodeKind(nd["kind"]), properties=nd.get("properties", {})))
    for ed in d.get("edges", []):
        from dataclasses import replace
        edge = Edge(
            id=ed["id"],
            kind=EdgeKind(ed["kind"]),
            source_id=ed["source_id"],
            target_id=ed["target_id"],
            properties=ed.get("properties", {}),
        )
        g._edges.append(edge)
    return g


# ---------------------------------------------------------------------------
# JSON file store
# ---------------------------------------------------------------------------

class JsonFileGraphStore(GraphStore):
    """Persist the knowledge graph as a single JSON file.

    Suitable for local development, small deployments, and testing.  Not
    suitable for concurrent multi-process writes.

    Args:
        path: Path to the JSON file.  Created automatically if absent.
    """

    def __init__(self, path: str | pathlib.Path) -> None:
        self._path = pathlib.Path(path)

    async def save(self, graph: KnowledgeGraph) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(_graph_to_dict(graph), indent=2))

    async def load(self) -> KnowledgeGraph:
        if not self._path.exists():
            return KnowledgeGraph()
        return _dict_to_graph(json.loads(self._path.read_text()))

    async def merge_event(self, incoming: KnowledgeGraph) -> None:
        """Merge *incoming* nodes/edges into the stored graph and re-save."""
        stored = await self.load()
        # Merge nodes (deduplicate by id).
        for node in incoming.nodes:
            stored.ensure_node(node.id, node.kind, **node.properties)
        # Append all edges from incoming (duplicates kept — same semantics as
        # in-memory graph).
        for edge in incoming.edges:
            stored._edges.append(edge)
        await self.save(stored)


# ---------------------------------------------------------------------------
# Redis store
# ---------------------------------------------------------------------------

class RedisGraphStore(GraphStore):
    """Persist the knowledge graph in Redis as a single JSON blob.

    Key: ``on:pipes:lineage:graph``

    Requires the ``redis`` optional extra::

        pip install 'openneuronic-pipes[redis]'
    """

    _KEY = "on:pipes:lineage:graph"

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        try:
            import redis.asyncio  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "redis[asyncio] is required for RedisGraphStore. "
                "Install with: pip install 'openneuronic-pipes[redis]'"
            ) from exc
        self._url = redis_url
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(self._url, decode_responses=True)
        return self._redis

    async def save(self, graph: KnowledgeGraph) -> None:
        r = await self._client()
        await r.set(self._KEY, json.dumps(_graph_to_dict(graph)))

    async def load(self) -> KnowledgeGraph:
        r = await self._client()
        raw = await r.get(self._KEY)
        if raw is None:
            return KnowledgeGraph()
        return _dict_to_graph(json.loads(raw))

    async def merge_event(self, incoming: KnowledgeGraph) -> None:
        """Merge *incoming* into the stored graph using a Redis pipeline."""
        stored = await self.load()
        for node in incoming.nodes:
            stored.ensure_node(node.id, node.kind, **node.properties)
        for edge in incoming.edges:
            stored._edges.append(edge)
        await self.save(stored)
