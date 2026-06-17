"""Replay endpoints: list points, get point, run replay."""
from __future__ import annotations

import asyncio
from typing import Any

from flask import Blueprint, current_app, jsonify, request

from openneuronic.pipes.api.serializers import replay_point_to_dict, run_result_to_dict
from openneuronic.pipes.replay.manifest import ReplayManifest
from openneuronic.pipes.replay.runner import ReplayRunner, ReplayValidationError

replay_bp = Blueprint("replay", __name__, url_prefix="/replay")


def _state() -> dict[str, Any]:
    return current_app.extensions["onpipes"]


# ---------------------------------------------------------------------------
# Replay points
# ---------------------------------------------------------------------------


@replay_bp.get("/points")
def list_points() -> tuple:
    state = _state()
    store = state["replay_store"]

    async def _list() -> list:
        # InMemoryReplayStore exposes _store dict; use the abstract list_for_pipe
        # by iterating all stored points via the internal dict.
        all_points = list(store._store.values())
        return sorted(all_points, key=lambda p: p.created_at, reverse=True)

    points = asyncio.run(_list())
    return jsonify([replay_point_to_dict(p) for p in points]), 200


@replay_bp.get("/points/<replay_id>")
def get_point(replay_id: str) -> tuple:
    state = _state()

    async def _load() -> Any:
        return await state["replay_store"].load(replay_id)

    point = asyncio.run(_load())
    if point is None:
        return jsonify({"error": f"ReplayPoint '{replay_id}' not found"}), 404
    return jsonify(replay_point_to_dict(point)), 200


# ---------------------------------------------------------------------------
# Run replay
# ---------------------------------------------------------------------------


@replay_bp.post("/run")
def run_replay() -> tuple:
    state = _state()
    registry = state["registry"]

    body: dict[str, Any] = request.get_json(silent=True) or {}

    pipe_id = body.get("pipe_id")
    if not pipe_id:
        return jsonify({"error": "'pipe_id' is required"}), 400
    pipe = registry.pipes.get(pipe_id)
    if pipe is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404

    replay_point_id = body.get("replay_point_id")
    if not replay_point_id:
        return jsonify({"error": "'replay_point_id' is required"}), 400

    mode = body.get("mode", "bookmark")
    scope: dict[str, Any] = body.get("scope", {})
    sink_strategy = body.get("sink_strategy", "dry_run")
    reason = body.get("reason")
    allow_config_drift: bool = bool(body.get("allow_config_drift", False))
    timeout_s: float | None = body.get("timeout_s")

    manifest = ReplayManifest(
        replay_point_id=replay_point_id,
        mode=mode,
        scope=scope,
        source_snapshot={},
        sink_strategy=sink_strategy,
        reason=reason,
    )

    runner = ReplayRunner(
        replay_store=state["replay_store"],
        allow_config_drift=allow_config_drift,
    )

    async def _run() -> Any:
        return await runner.run(pipe, manifest)

    coro = _run()
    try:
        if timeout_s is not None:
            async def _with_timeout() -> Any:
                return await asyncio.wait_for(coro, timeout=timeout_s)
            result = asyncio.run(_with_timeout())
        else:
            result = asyncio.run(coro)
    except ReplayValidationError as exc:
        return jsonify({"error": str(exc)}), 422
    except asyncio.TimeoutError:
        return jsonify({"error": "Replay timed out"}), 504

    result_dict = run_result_to_dict(result)
    state["run_history"].append(result_dict)
    status_code = 200 if result.success else 500
    return jsonify(result_dict), status_code
