"""Opus endpoints: list, detail, run."""
from __future__ import annotations

import asyncio
from typing import Any

from flask import Blueprint, current_app, jsonify, request

from openneuronic.pipes.api.serializers import opus_result_to_dict, opus_to_dict
from openneuronic.pipes.opus.runner import OpusRunner

opus_bp = Blueprint("opus", __name__, url_prefix="/opus")


def _state() -> dict[str, Any]:
    return current_app.extensions["onpipes"]


# ---------------------------------------------------------------------------
# List & detail
# ---------------------------------------------------------------------------


@opus_bp.get("/")
def list_opus() -> tuple:
    registry = _state()["registry"]
    return jsonify([opus_to_dict(o) for o in registry.opus]), 200


@opus_bp.get("/<opus_id>")
def get_opus(opus_id: str) -> tuple:
    registry = _state()["registry"]
    opus = registry.opus.get(opus_id)
    if opus is None:
        return jsonify({"error": f"Opus '{opus_id}' not found"}), 404
    return jsonify(opus_to_dict(opus)), 200


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


@opus_bp.post("/<opus_id>/run")
def run_opus(opus_id: str) -> tuple:
    state = _state()
    registry = state["registry"]
    opus = registry.opus.get(opus_id)
    if opus is None:
        return jsonify({"error": f"Opus '{opus_id}' not found"}), 404

    body: dict[str, Any] = request.get_json(silent=True) or {}
    run_id: str | None = body.get("run_id")
    batch_size: int = int(body.get("batch_size", 500))
    timeout_s: float | None = body.get("timeout_s")

    runner = OpusRunner(batch_size=batch_size)

    async def _run() -> Any:
        return await runner.run(opus, run_id=run_id)

    coro = _run()
    if timeout_s is not None:
        async def _with_timeout() -> Any:
            return await asyncio.wait_for(coro, timeout=timeout_s)
        result = asyncio.run(_with_timeout())
    else:
        result = asyncio.run(coro)

    result_dict = opus_result_to_dict(result)

    # Flatten individual segment run results into the shared run history.
    for seg_result in result.segment_results.values():
        if seg_result.run_result is not None:
            from openneuronic.pipes.api.serializers import run_result_to_dict
            state["run_history"].append(run_result_to_dict(seg_result.run_result))

    status_code = 200 if result.success else 500
    return jsonify(result_dict), status_code
