"""In-memory knowledge graph — nodes and typed edges."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

try:
    from enum import StrEnum
except ImportError:  # Python < 3.11
    from enum import Enum

    class StrEnum(str, Enum):  # type: ignore[no-redef]
        pass


from openneuronic.pipes.lineage.events import LineageEvent, LineageEventKind


class NodeKind(StrEnum):
    PIPE             = "pipe"
    SEGMENT          = "segment"
    OPUS             = "opus"
    RUN              = "run"
    DATASET          = "dataset"
    COLUMN           = "column"
    SCHEMA_VERSION   = "schema_version"
    CONTRACT_VERSION = "contract_version"
    PROCESSOR        = "processor"
    GUARD_RESULT     = "guard_result"
    MEASURE          = "measure"


class EdgeKind(StrEnum):
    READS_FROM           = "READS_FROM"
    WRITES_TO            = "WRITES_TO"
    DERIVES_FROM         = "DERIVES_FROM"
    PART_OF              = "PART_OF"
    EXECUTED_IN          = "EXECUTED_IN"
    UPGRADED_FROM_SCHEMA = "UPGRADED_FROM_SCHEMA"
    VALIDATED_BY_CONTRACT = "VALIDATED_BY_CONTRACT"
    PROTECTED_BY_GUARD   = "PROTECTED_BY_GUARD"
    MEASURED_BY          = "MEASURED_BY"


@dataclass
class Node:
    id: str
    kind: NodeKind
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    id: str
    kind: EdgeKind
    source_id: str
    target_id: str
    properties: dict[str, Any] = field(default_factory=dict)


class KnowledgeGraph:
    """In-memory graph of nodes and typed edges.

    Nodes are deduplicated by ``id``; multiple edges of the same kind between
    the same pair of nodes are allowed (e.g. multiple READS_FROM events across
    different runs).
    """

    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(self, node: Node) -> Node:
        self._nodes[node.id] = node
        return node

    def ensure_node(
        self, node_id: str, kind: NodeKind, **properties: Any
    ) -> Node:
        """Return existing node or create it if absent."""
        if node_id not in self._nodes:
            self.add_node(Node(id=node_id, kind=kind, properties=dict(properties)))
        return self._nodes[node_id]

    def add_edge(
        self,
        kind: EdgeKind,
        source_id: str,
        target_id: str,
        **properties: Any,
    ) -> Edge:
        edge = Edge(
            id=str(uuid.uuid4()),
            kind=kind,
            source_id=source_id,
            target_id=target_id,
            properties=dict(properties),
        )
        self._edges.append(edge)
        return edge

    # ------------------------------------------------------------------
    # Lineage event ingestion
    # ------------------------------------------------------------------

    def add_event(self, event: LineageEvent) -> None:
        """Translate a :class:`~openneuronic.pipes.lineage.events.LineageEvent`
        into graph nodes and edges."""
        run_id = f"run:{event.run_id}"
        self.ensure_node(run_id, NodeKind.RUN, run_id=event.run_id)

        pipe_id = f"pipe:{event.pipe_id}"
        if pipe_id not in self._nodes:
            self.ensure_node(pipe_id, NodeKind.PIPE, pipe_id=event.pipe_id)
            self.add_edge(EdgeKind.EXECUTED_IN, pipe_id, run_id)

        if event.kind == LineageEventKind.READ and event.source_ref:
            ds_id = f"dataset:{event.source_ref.name}"
            self.ensure_node(ds_id, NodeKind.DATASET, name=event.source_ref.name)
            self.add_edge(
                EdgeKind.READS_FROM,
                pipe_id,
                ds_id,
                run_id=event.run_id,
                schema_version=event.schema_version,
            )

        elif event.kind == LineageEventKind.WRITE and event.target_ref:
            ds_id = f"dataset:{event.target_ref.name}"
            self.ensure_node(ds_id, NodeKind.DATASET, name=event.target_ref.name)
            self.add_edge(
                EdgeKind.WRITES_TO,
                pipe_id,
                ds_id,
                run_id=event.run_id,
                schema_version=event.schema_version,
                contract_version=event.contract_version,
            )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> Node | None:
        return self._nodes.get(node_id)

    def edges_from(self, node_id: str) -> list[Edge]:
        return [e for e in self._edges if e.source_id == node_id]

    def edges_to(self, node_id: str) -> list[Edge]:
        return [e for e in self._edges if e.target_id == node_id]

    def edges_of_kind(self, kind: EdgeKind) -> list[Edge]:
        return [e for e in self._edges if e.kind == kind]

    @property
    def nodes(self) -> list[Node]:
        return list(self._nodes.values())

    @property
    def edges(self) -> list[Edge]:
        return list(self._edges)
