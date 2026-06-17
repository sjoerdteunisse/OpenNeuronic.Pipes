"""Secrets management endpoints.

All endpoints require the ``X-API-Key`` header whose value must match the
``ONPIPES_API_KEY`` environment variable.  If the variable is not set the
endpoints respond with ``503 Service Unavailable``.

Secret *values* are **never** returned in any response.
"""
from __future__ import annotations

import os
from functools import wraps
from typing import Any

from flask import Blueprint, current_app, jsonify, request

from openneuronic.pipes.api.secrets import SecretKeyError, verify_api_key

secrets_bp = Blueprint("secrets", __name__, url_prefix="/secrets")

_API_KEY_HEADER = "X-API-Key"


# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------


def _require_key(fn):
    """Decorator that enforces API-key authentication on a route."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        expected = os.environ.get("ONPIPES_API_KEY", "").strip()
        if not expected:
            return (
                jsonify(
                    {
                        "error": (
                            "Secrets management is disabled. "
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


def _store() -> Any:
    return current_app.extensions["onpipes"]["secret_store"]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@secrets_bp.get("/")
@_require_key
def list_secrets() -> tuple:
    """Return all stored secret *key names* (never values)."""
    return jsonify({"keys": _store().keys()}), 200


@secrets_bp.put("/<key>")
@_require_key
def set_secret(key: str) -> tuple:
    """Create or overwrite a secret.

    Body: ``{"value": "<plaintext secret>"}``

    The value is stored in memory and never echoed back.
    """
    body = request.get_json(silent=True) or {}
    value = body.get("value")
    if value is None:
        return jsonify({"error": "'value' field is required in the request body"}), 400
    if not isinstance(value, str):
        return jsonify({"error": "'value' must be a string"}), 400

    try:
        _store().set(key, value)
    except SecretKeyError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify({"key": key, "stored": True}), 201


@secrets_bp.delete("/<key>")
@_require_key
def delete_secret(key: str) -> tuple:
    """Delete a secret by key.  No-op if the key does not exist."""
    _store().delete(key)
    return "", 204
