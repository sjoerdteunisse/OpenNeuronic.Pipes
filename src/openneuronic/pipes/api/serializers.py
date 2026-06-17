"""JSON-safe serializers for OpenNeuronic.Pipes dataclasses."""
from __future__ import annotations

import datetime
from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.lineage.graph.model import KnowledgeGraph
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.runner import OpusRunResult, SegmentRunResult
from openneuronic.pipes.replay.point import ReplayPoint
from openneuronic.pipes.runner import RunResult


def _dt(value: datetime.datetime | None) -> str | None:
    """Return an ISO-8601 string with UTC timezone, or None."""
    if value is None:
        return None
    return value.isoformat()


def run_result_to_dict(result: RunResult) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.runner.RunResult` to a JSON-safe dict."""
    return {
        "pipe_id": result.pipe_id,
        "run_id": result.run_id,
        "records_read": result.records_read,
        "records_written": result.records_written,
        "records_dropped": result.records_dropped,
        "started_at": _dt(result.started_at),
        "finished_at": _dt(result.finished_at),
        "success": result.success,
        "error": str(result.error) if result.error else None,
        "metrics": run_metrics_to_dict(result.metrics) if result.metrics else None,
        "lineage_event_count": len(result.lineage) if result.lineage else 0,
        "replay_point_id": result.replay_point.replay_id if result.replay_point else None,
    }


def run_metrics_to_dict(metrics: Any) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.metrics.base.RunMetrics` to a dict."""
    return {
        "pipe_id": metrics.pipe_id,
        "records_read": metrics.records_read,
        "records_written": metrics.records_written,
        "records_dropped": metrics.records_dropped,
        "batches": metrics.batches,
        "errors": metrics.errors,
        "duration_ms": metrics.duration_ms,
        "throughput_rps": metrics.throughput_rps,
        "null_rates": metrics.null_rates,
        "duplicate_rate": metrics.duplicate_rate,
        "row_count_deviation": metrics.row_count_deviation,
    }


def segment_result_to_dict(result: SegmentRunResult) -> dict[str, Any]:
    return {
        "segment_id": result.segment_id,
        "success": result.success,
        "retries": result.retries,
        "error": result.error,
        "run_result": run_result_to_dict(result.run_result) if result.run_result else None,
    }


def opus_result_to_dict(result: OpusRunResult) -> dict[str, Any]:
    """Serialize an :class:`~openneuronic.pipes.opus.runner.OpusRunResult` to a dict."""
    return {
        "opus_id": result.opus_id,
        "run_id": result.run_id,
        "success": result.success,
        "started_at": _dt(result.started_at),
        "finished_at": _dt(result.finished_at),
        "failed_segments": result.failed_segments,
        "segment_results": {
            sid: segment_result_to_dict(sr)
            for sid, sr in result.segment_results.items()
        },
    }


def bookmark_to_dict(bookmark: Bookmark) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.core.bookmark.Bookmark` to a dict."""
    value = bookmark.value
    if isinstance(value, (datetime.datetime, datetime.date)):
        value = value.isoformat()
    elif isinstance(value, bytes):
        value = value.hex()

    previous = bookmark.previous_value
    if isinstance(previous, (datetime.datetime, datetime.date)):
        previous = previous.isoformat()
    elif isinstance(previous, bytes):
        previous = previous.hex()

    return {
        "pipe_id": bookmark.pipe_id,
        "column": bookmark.column,
        "type": str(bookmark.type),
        "value": value,
        "previous_value": previous,
        "batch_count": bookmark.batch_count,
    }


def pipe_to_dict(pipe: Pipe) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.core.pipe.Pipe` to a dict."""
    return {
        "id": pipe.id,
        "mode": str(pipe.mode),
        "source_type": type(pipe.source).__name__,
        "sink_type": type(pipe.sink).__name__,
        "processor_count": len(pipe.processors),
        "guard_count": len(pipe.guards),
        "measure_count": len(pipe.measures),
        "has_schema": pipe.schema is not None,
        "has_contract": pipe.contract is not None,
        "has_scope": pipe.scope is not None,
    }


def opus_to_dict(opus: Opus) -> dict[str, Any]:
    """Serialize an :class:`~openneuronic.pipes.opus.opus.Opus` to a dict."""
    segments = []
    for seg in opus._segments.values():
        segments.append({
            "id": seg.id,
            "pipe_id": seg.pipe.id,
            "depends_on": seg.depends_on,
            "wait_strategy": str(seg.wait_strategy),
        })
    return {
        "id": opus.id,
        "schedule": opus.schedule,
        "durable": opus.durable,
        "on_failure": str(opus.on_failure),
        "segment_count": len(segments),
        "segments": segments,
    }


def replay_point_to_dict(point: ReplayPoint) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.replay.point.ReplayPoint` to a dict."""
    return {
        "replay_id": point.replay_id,
        "pipe_id": point.pipe_id,
        "opus_id": point.opus_id,
        "config_digest": point.config_digest,
        "source_bookmarks": point.source_bookmarks,
        "schema_versions": point.schema_versions,
        "contract_versions": point.contract_versions,
        "created_at": _dt(point.created_at),
        "lineage_ref": point.lineage_ref,
        "manifest_ref": point.manifest_ref,
    }


def knowledge_graph_to_dict(graph: KnowledgeGraph) -> dict[str, Any]:
    """Serialize a :class:`~openneuronic.pipes.lineage.graph.model.KnowledgeGraph` to a dict."""
    nodes = [
        {"id": n.id, "kind": str(n.kind), "properties": n.properties}
        for n in graph._nodes.values()
    ]
    edges = [
        {
            "id": e.id,
            "kind": str(e.kind),
            "source_id": e.source_id,
            "target_id": e.target_id,
            "properties": e.properties,
        }
        for e in graph._edges
    ]
    return {"nodes": nodes, "edges": edges}
