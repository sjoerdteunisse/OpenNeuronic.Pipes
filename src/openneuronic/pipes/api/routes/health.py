"""Health and version endpoints."""
from __future__ import annotations

from flask import Blueprint, jsonify

try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        _VERSION = version("openneuronic-pipes")
    except PackageNotFoundError:
        _VERSION = "0.1.0"
except ImportError:
    _VERSION = "0.1.0"

health_bp = Blueprint("health", __name__)


@health_bp.get("/health")
def health() -> tuple:
    return jsonify({"status": "ok", "version": _VERSION}), 200


@health_bp.get("/version")
def get_version() -> tuple:
    return jsonify({"version": _VERSION}), 200
