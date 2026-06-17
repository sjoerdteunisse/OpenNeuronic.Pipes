"""Tests for /health and /version endpoints."""
from __future__ import annotations

import pytest


def test_health_returns_ok(client) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert "version" in data


def test_version_endpoint(client) -> None:
    resp = client.get("/version")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "version" in data


def test_unknown_route_returns_404(client) -> None:
    resp = client.get("/does-not-exist")
    assert resp.status_code == 404
