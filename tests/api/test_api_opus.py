"""Tests for /opus endpoints."""
from __future__ import annotations

import pytest


def test_list_opus(client) -> None:
    resp = client.get("/opus/")
    assert resp.status_code == 200
    items = resp.get_json()
    assert isinstance(items, list)
    assert any(o["id"] == "test-opus" for o in items)


def test_get_opus_detail(client) -> None:
    resp = client.get("/opus/test-opus")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["id"] == "test-opus"
    assert data["segment_count"] == 1
    assert len(data["segments"]) == 1
    assert data["segments"][0]["id"] == "step-1"


def test_get_unknown_opus_returns_404(client) -> None:
    resp = client.get("/opus/ghost")
    assert resp.status_code == 404


def test_run_opus_success(client) -> None:
    resp = client.post("/opus/test-opus/run", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert "step-1" in data["segment_results"]
    assert data["segment_results"]["step-1"]["success"] is True


def test_run_unknown_opus_returns_404(client) -> None:
    resp = client.post("/opus/ghost/run", json={})
    assert resp.status_code == 404


def test_run_opus_populates_run_history(client) -> None:
    client.post("/opus/test-opus/run", json={})
    resp = client.get("/runs/")
    assert resp.status_code == 200
    history = resp.get_json()
    assert len(history) >= 1
