"""Tests for /pipes endpoints."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# List / detail
# ---------------------------------------------------------------------------


def test_list_pipes_returns_registered_pipe(client) -> None:
    resp = client.get("/pipes/")
    assert resp.status_code == 200
    pipes = resp.get_json()
    assert isinstance(pipes, list)
    assert any(p["id"] == "test-pipe" for p in pipes)


def test_list_pipes_includes_mode_and_types(client) -> None:
    resp = client.get("/pipes/")
    pipe = resp.get_json()[0]
    assert "mode" in pipe
    assert "source_type" in pipe
    assert "sink_type" in pipe


def test_get_pipe_detail(client) -> None:
    resp = client.get("/pipes/test-pipe")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["id"] == "test-pipe"


def test_get_unknown_pipe_returns_404(client) -> None:
    resp = client.get("/pipes/does-not-exist")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def test_run_pipe_success(client) -> None:
    resp = client.post("/pipes/test-pipe/run", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["records_written"] == 3  # MemorySource yields 3 records


def test_run_pipe_stores_run_id(client) -> None:
    resp = client.post("/pipes/test-pipe/run", json={})
    data = resp.get_json()
    assert "run_id" in data
    assert len(data["run_id"]) == 36  # UUID4


def test_run_pipe_with_custom_batch_size(client) -> None:
    resp = client.post("/pipes/test-pipe/run", json={"batch_size": 1})
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True


def test_run_unknown_pipe_returns_404(client) -> None:
    resp = client.post("/pipes/ghost/run", json={})
    assert resp.status_code == 404


def test_run_pipe_invalid_bookmark_returns_400(client) -> None:
    resp = client.post(
        "/pipes/test-pipe/run",
        json={"bookmark": {"type": "integer"}},  # missing required "column" and "value"
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Bookmark
# ---------------------------------------------------------------------------


def test_get_bookmark_before_run_returns_404(client) -> None:
    resp = client.get("/pipes/test-pipe/bookmark")
    assert resp.status_code == 404


def test_reset_bookmark_unknown_pipe_returns_404(client) -> None:
    resp = client.delete("/pipes/ghost/bookmark")
    assert resp.status_code == 404


def test_reset_bookmark_no_content(client) -> None:
    # Resetting a pipe with no bookmark should still return 204.
    resp = client.delete("/pipes/test-pipe/bookmark")
    assert resp.status_code == 204
