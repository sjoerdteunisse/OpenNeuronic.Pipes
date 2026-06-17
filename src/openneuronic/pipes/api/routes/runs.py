"""Run history endpoints."""
from __future__ import annotations

from typing import Any

from flask import Blueprint, current_app, jsonify

runs_bp = Blueprint("runs", __name__, url_prefix="/runs")


def _state() -> dict[str, Any]:
    return current_app.extensions["onpipes"]


@runs_bp.get("/")
def list_runs() -> tuple:
    history = _state()["run_history"]
    return jsonify(list(history)), 200


@runs_bp.get("/<run_id>")
def get_run(run_id: str) -> tuple:
    history = _state()["run_history"]
    for entry in history:
        if entry.get("run_id") == run_id:
            return jsonify(entry), 200
    return jsonify({"error": f"Run '{run_id}' not found"}), 404
