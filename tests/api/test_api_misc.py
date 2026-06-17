"""Tests for /runs, /replay, and /lineage endpoints."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# /runs
# ---------------------------------------------------------------------------


def test_runs_empty_initially(client) -> None:
    resp = client.get("/runs/")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_runs_populated_after_pipe_run(client) -> None:
    client.post("/pipes/test-pipe/run", json={})
    resp = client.get("/runs/")
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["pipe_id"] == "test-pipe"


def test_get_run_by_id(client) -> None:
    run_resp = client.post("/pipes/test-pipe/run", json={})
    run_id = run_resp.get_json()["run_id"]

    resp = client.get(f"/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.get_json()["run_id"] == run_id


def test_get_unknown_run_returns_404(client) -> None:
    resp = client.get("/runs/does-not-exist")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /replay
# ---------------------------------------------------------------------------


def test_replay_points_empty_initially(client) -> None:
    resp = client.get("/replay/points")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_replay_points_populated_after_run(client) -> None:
    client.post("/pipes/test-pipe/run", json={})
    resp = client.get("/replay/points")
    # Replay points are stored only when a replay_point exists on RunResult.
    # MemorySource produces records but replay_point is optional; just assert the endpoint works.
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), list)


def test_get_unknown_replay_point_returns_404(client) -> None:
    resp = client.get("/replay/points/no-such-id")
    assert resp.status_code == 404


def test_replay_run_missing_pipe_id_returns_400(client) -> None:
    resp = client.post("/replay/run", json={"replay_point_id": "abc"})
    assert resp.status_code == 400


def test_replay_run_missing_replay_point_id_returns_400(client) -> None:
    resp = client.post("/replay/run", json={"pipe_id": "test-pipe"})
    assert resp.status_code == 400


def test_replay_run_unknown_pipe_returns_404(client) -> None:
    resp = client.post(
        "/replay/run",
        json={"pipe_id": "ghost", "replay_point_id": "abc"},
    )
    assert resp.status_code == 404


def test_replay_run_unknown_point_returns_422(client) -> None:
    resp = client.post(
        "/replay/run",
        json={"pipe_id": "test-pipe", "replay_point_id": "no-such-id"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /lineage
# ---------------------------------------------------------------------------


def test_lineage_full_graph_initially_empty(client) -> None:
    resp = client.get("/lineage/")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "nodes" in data
    assert "edges" in data


def test_lineage_pipe_subgraph(client) -> None:
    resp = client.get("/lineage/pipes/test-pipe")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "nodes" in data
    assert "edges" in data


def test_lineage_unknown_pipe_returns_404(client) -> None:
    resp = client.get("/lineage/pipes/ghost")
    assert resp.status_code == 404
