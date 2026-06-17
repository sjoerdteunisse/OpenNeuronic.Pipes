"""Pipe endpoints: list, create, detail, run, bookmark show/reset."""
from __future__ import annotations

import asyncio
import os
from functools import wraps
from typing import Any

from flask import Blueprint, current_app, jsonify, request

from openneuronic.pipes.api.factory import FactoryError, build_pipe
from openneuronic.pipes.api.secrets import SecretNotFoundError, verify_api_key
from openneuronic.pipes.api.serializers import (
    bookmark_to_dict,
    pipe_to_dict,
    run_result_to_dict,
)
from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import BookmarkType
from openneuronic.pipes.runner import LocalRunner

pipes_bp = Blueprint("pipes", __name__, url_prefix="/pipes")

_API_KEY_HEADER = "X-API-Key"


# ---------------------------------------------------------------------------
# Auth guard (reused from secrets, applied only to write operations)
# ---------------------------------------------------------------------------


def _require_key(fn):
    """Enforce API-key on mutating pipe endpoints (create / delete)."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        expected = os.environ.get("ONPIPES_API_KEY", "").strip()
        if not expected:
            return (
                jsonify(
                    {
                        "error": (
                            "Pipe creation requires an API key. "
                            "Set the ONPIPES_API_KEY environment variable to enable it."
                        )
                    }
                ),
                503,
            )
        provided = request.headers.get(_API_KEY_HEADER, "")
        if not provided or not verify_api_key(provided, expected):
            return jsonify({"error": "Invalid or missing X-API-Key header"}), 401
        return fn(*args, **kwargs)

    return wrapper


def _state() -> dict[str, Any]:
    return current_app.extensions["onpipes"]


# ---------------------------------------------------------------------------
# List & detail
# ---------------------------------------------------------------------------


@pipes_bp.get("/")
def list_pipes() -> tuple:
    registry = _state()["registry"]
    return jsonify([pipe_to_dict(p) for p in registry.pipes]), 200


@pipes_bp.get("/<pipe_id>")
def get_pipe(pipe_id: str) -> tuple:
    registry = _state()["registry"]
    pipe = registry.pipes.get(pipe_id)
    if pipe is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404
    return jsonify(pipe_to_dict(pipe)), 200


# ---------------------------------------------------------------------------
# Create pipe
# ---------------------------------------------------------------------------


@pipes_bp.post("/")
@_require_key
def create_pipe() -> tuple:
    """Dynamically register a new pipe from a JSON spec.

    Requires the ``X-API-Key`` header.

    Body example::

        {
            "id":   "orders-sync",
            "mode": "incremental",
            "source": {
                "type":       "sqlserver",
                "connection": {"$secret": "src_conn"},
                "query":      "SELECT * FROM orders WHERE updated_at > ?"
            },
            "sink": {
                "type":         "sqlserver",
                "connection":   {"$secret": "dst_conn"},
                "target_table": "orders_dest",
                "upsert_key":   "id"
            }
        }

    Use ``"type": "memory"`` with ``"payloads": [...]`` for a lightweight
    in-process source/sink when no real database is available.
    """
    state = _state()
    body: dict[str, Any] = request.get_json(silent=True) or {}

    try:
        pipe = build_pipe(body, state["secret_store"])
    except FactoryError as exc:
        return jsonify({"error": str(exc)}), 400
    except SecretNotFoundError as exc:
        return jsonify({"error": str(exc)}), 422

    if pipe.id in state["registry"].pipes:
        return jsonify({"error": f"Pipe '{pipe.id}' already exists. Delete it first."}), 409

    state["registry"].pipes.register(pipe)
    return jsonify(pipe_to_dict(pipe)), 201


@pipes_bp.delete("/<pipe_id>")
@_require_key
def delete_pipe(pipe_id: str) -> tuple:
    """Deregister a dynamically created pipe.

    Requires the ``X-API-Key`` header.
    Pipes registered at startup via the :class:`~openneuronic.pipes.api.registry.Registry`
    can also be removed this way.  In-flight runs are not interrupted.
    """
    state = _state()
    if state["registry"].pipes.get(pipe_id) is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404
    # Remove from the internal dict directly.
    state["registry"].pipes._pipes.pop(pipe_id, None)
    return "", 204


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


@pipes_bp.post("/<pipe_id>/run")
def run_pipe(pipe_id: str) -> tuple:
    state = _state()
    registry = state["registry"]
    pipe = registry.pipes.get(pipe_id)
    if pipe is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404

    body: dict[str, Any] = request.get_json(silent=True) or {}
    batch_size: int = int(body.get("batch_size", 500))
    timeout_s: float | None = body.get("timeout_s")

    # Optionally restore a bookmark from the request body.
    bookmark: Bookmark | None = None
    bm_data = body.get("bookmark")
    if bm_data:
        try:
            bookmark = Bookmark(
                pipe_id=pipe_id,
                column=bm_data["column"],
                type=BookmarkType(bm_data["type"]),
                value=bm_data["value"],
                previous_value=bm_data.get("previous_value"),
            )
        except (KeyError, ValueError) as exc:
            return jsonify({"error": f"Invalid bookmark: {exc}"}), 400
    else:
        # Load persisted bookmark if one exists.
        bookmark = asyncio.run(state["bookmark_store"].load(pipe_id))

    runner = LocalRunner(batch_size=batch_size)

    async def _run() -> Any:
        result = await runner.run(pipe, bookmark=bookmark)
        # Persist bookmark advance if the run succeeded and produced a replay point.
        if result.success and result.replay_point:
            await state["replay_store"].save(result.replay_point)
        # Save updated bookmark.
        if result.success and bookmark is not None:
            await state["bookmark_store"].save(bookmark)
        return result

    coro = _run()
    if timeout_s is not None:
        async def _with_timeout() -> Any:
            return await asyncio.wait_for(coro, timeout=timeout_s)
        result = asyncio.run(_with_timeout())
    else:
        result = asyncio.run(coro)

    result_dict = run_result_to_dict(result)
    state["run_history"].append(result_dict)

    status_code = 200 if result.success else 500
    return jsonify(result_dict), status_code


# ---------------------------------------------------------------------------
# Bookmark
# ---------------------------------------------------------------------------


@pipes_bp.get("/<pipe_id>/bookmark")
def get_bookmark(pipe_id: str) -> tuple:
    state = _state()
    registry = state["registry"]
    if registry.pipes.get(pipe_id) is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404

    bookmark = asyncio.run(state["bookmark_store"].load(pipe_id))
    if bookmark is None:
        return jsonify({"error": f"No bookmark found for pipe '{pipe_id}'"}), 404
    return jsonify(bookmark_to_dict(bookmark)), 200


@pipes_bp.delete("/<pipe_id>/bookmark")
def reset_bookmark(pipe_id: str) -> tuple:
    state = _state()
    registry = state["registry"]
    if registry.pipes.get(pipe_id) is None:
        return jsonify({"error": f"Pipe '{pipe_id}' not found"}), 404

    asyncio.run(state["bookmark_store"].delete(pipe_id))
    return "", 204
