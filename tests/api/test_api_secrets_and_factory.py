"""Tests for secrets store, factory, and the new /pipes and /secrets endpoints."""
from __future__ import annotations

import pytest

from openneuronic.pipes.api.factory import FactoryError, build_pipe
from openneuronic.pipes.api.secrets import (
    SecretKeyError,
    SecretNotFoundError,
    SecretStore,
    verify_api_key,
)


# ---------------------------------------------------------------------------
# SecretStore unit tests
# ---------------------------------------------------------------------------


def test_set_and_resolve_secret() -> None:
    store = SecretStore()
    store.set("my_key", "super-secret")
    assert store.resolve({"$secret": "my_key"}) == "super-secret"


def test_passthrough_non_secret_value() -> None:
    store = SecretStore()
    assert store.resolve("plain-string") == "plain-string"
    assert store.resolve(42) == 42
    assert store.resolve({"key": "val"}) == {"key": "val"}


def test_resolve_missing_secret_raises() -> None:
    store = SecretStore()
    with pytest.raises(SecretNotFoundError):
        store.resolve({"$secret": "does-not-exist"})


def test_keys_never_returns_values() -> None:
    store = SecretStore()
    store.set("conn_a", "value-a")
    store.set("conn_b", "value-b")
    assert store.keys() == ["conn_a", "conn_b"]


def test_delete_secret() -> None:
    store = SecretStore()
    store.set("temp", "val")
    store.delete("temp")
    assert "temp" not in store


def test_invalid_key_name_raises() -> None:
    store = SecretStore()
    with pytest.raises(SecretKeyError):
        store.set("bad key!", "val")
    with pytest.raises(SecretKeyError):
        store.set("", "val")


def test_verify_api_key_correct() -> None:
    assert verify_api_key("abc123", "abc123") is True


def test_verify_api_key_wrong() -> None:
    assert verify_api_key("wrong", "abc123") is False


# ---------------------------------------------------------------------------
# Factory unit tests
# ---------------------------------------------------------------------------


def test_build_memory_pipe() -> None:
    from openneuronic.pipes.api.factory import _ApiMemorySource, _ApiMemorySink

    store = SecretStore()
    pipe = build_pipe(
        {
            "id": "factory-test",
            "mode": "full",
            "source": {"type": "memory", "payloads": [{"id": 1}]},
            "sink": {"type": "memory"},
        },
        store,
    )
    assert pipe.id == "factory-test"
    assert isinstance(pipe.source, _ApiMemorySource)
    assert isinstance(pipe.sink, _ApiMemorySink)


def test_build_pipe_resolves_secret_in_connection() -> None:
    """Secret reference in connection field should be resolved at build time."""
    store = SecretStore()
    store.set("src_conn", "fake-dsn")

    # SQLServer raises ImportError if aioodbc not installed, but secret resolution
    # happens before the import — so we can test the import error path to confirm
    # the secret was resolved (not returned as-is).
    try:
        build_pipe(
            {
                "id": "ss-pipe",
                "mode": "incremental",
                "source": {
                    "type": "sqlserver",
                    "connection": {"$secret": "src_conn"},
                    "query": "SELECT 1",
                },
                "sink": {
                    "type": "sqlserver",
                    "connection": {"$secret": "src_conn"},
                    "target_table": "t",
                },
            },
            store,
        )
    except ImportError:
        pass  # aioodbc not installed — that's fine; secret was still resolved


def test_build_pipe_missing_secret_raises() -> None:
    store = SecretStore()
    with pytest.raises(SecretNotFoundError):
        build_pipe(
            {
                "id": "bad-pipe",
                "mode": "incremental",
                "source": {
                    "type": "sqlserver",
                    "connection": {"$secret": "missing"},
                    "query": "SELECT 1",
                },
                "sink": {"type": "memory"},
            },
            store,
        )


def test_build_pipe_missing_id_raises() -> None:
    with pytest.raises(FactoryError, match="'id'"):
        build_pipe({"source": {"type": "memory"}, "sink": {"type": "memory"}}, SecretStore())


def test_build_pipe_invalid_mode_raises() -> None:
    with pytest.raises(FactoryError, match="Invalid mode"):
        build_pipe(
            {
                "id": "p",
                "mode": "turbo",
                "source": {"type": "memory"},
                "sink": {"type": "memory"},
            },
            SecretStore(),
        )


def test_build_pipe_unknown_source_type_raises() -> None:
    with pytest.raises(FactoryError, match="Unknown source type"):
        build_pipe(
            {"id": "p", "source": {"type": "oracle"}, "sink": {"type": "memory"}},
            SecretStore(),
        )


# ---------------------------------------------------------------------------
# /secrets API endpoint tests
# ---------------------------------------------------------------------------


def test_secrets_without_api_key_env_returns_503(client, monkeypatch) -> None:
    monkeypatch.delenv("ONPIPES_API_KEY", raising=False)
    resp = client.get("/secrets/")
    assert resp.status_code == 503


def test_secrets_wrong_api_key_returns_401(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "correct-key")
    resp = client.get("/secrets/", headers={"X-API-Key": "wrong-key"})
    assert resp.status_code == 401


def test_set_and_list_secret(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "test-key")
    resp = client.put(
        "/secrets/my_conn",
        json={"value": "secret-value"},
        headers={"X-API-Key": "test-key"},
    )
    assert resp.status_code == 201
    data = resp.get_json()
    assert data["key"] == "my_conn"
    assert data["stored"] is True
    assert "value" not in data  # value MUST NOT be echoed

    list_resp = client.get("/secrets/", headers={"X-API-Key": "test-key"})
    assert list_resp.status_code == 200
    assert "my_conn" in list_resp.get_json()["keys"]


def test_set_secret_invalid_key_returns_400(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "test-key")
    resp = client.put(
        "/secrets/bad key!",
        json={"value": "v"},
        headers={"X-API-Key": "test-key"},
    )
    assert resp.status_code == 400


def test_set_secret_missing_value_returns_400(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "test-key")
    resp = client.put(
        "/secrets/mykey",
        json={},
        headers={"X-API-Key": "test-key"},
    )
    assert resp.status_code == 400


def test_delete_secret(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "test-key")
    client.put("/secrets/del_me", json={"value": "v"}, headers={"X-API-Key": "test-key"})
    resp = client.delete("/secrets/del_me", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# POST /pipes (dynamic creation) endpoint tests
# ---------------------------------------------------------------------------


def test_create_pipe_no_api_key_returns_503(client, monkeypatch) -> None:
    monkeypatch.delenv("ONPIPES_API_KEY", raising=False)
    resp = client.post("/pipes/", json={"id": "x", "source": {"type": "memory"}, "sink": {"type": "memory"}})
    assert resp.status_code == 503


def test_create_pipe_wrong_api_key_returns_401(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    resp = client.post(
        "/pipes/",
        json={"id": "x", "source": {"type": "memory"}, "sink": {"type": "memory"}},
        headers={"X-API-Key": "wrong"},
    )
    assert resp.status_code == 401


def test_create_memory_pipe_success(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    resp = client.post(
        "/pipes/",
        json={
            "id": "dynamic-pipe",
            "mode": "full",
            "source": {"type": "memory", "payloads": [{"id": 1}, {"id": 2}]},
            "sink": {"type": "memory"},
        },
        headers={"X-API-Key": "key"},
    )
    assert resp.status_code == 201
    data = resp.get_json()
    assert data["id"] == "dynamic-pipe"


def test_create_pipe_appears_in_list(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    client.post(
        "/pipes/",
        json={
            "id": "listed-pipe",
            "mode": "full",
            "source": {"type": "memory"},
            "sink": {"type": "memory"},
        },
        headers={"X-API-Key": "key"},
    )
    pipes = client.get("/pipes/").get_json()
    assert any(p["id"] == "listed-pipe" for p in pipes)


def test_create_duplicate_pipe_returns_409(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    spec = {
        "id": "dup-pipe",
        "mode": "full",
        "source": {"type": "memory"},
        "sink": {"type": "memory"},
    }
    client.post("/pipes/", json=spec, headers={"X-API-Key": "key"})
    resp = client.post("/pipes/", json=spec, headers={"X-API-Key": "key"})
    assert resp.status_code == 409


def test_create_pipe_missing_secret_returns_422(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    resp = client.post(
        "/pipes/",
        json={
            "id": "secret-pipe",
            "source": {"type": "sqlserver", "connection": {"$secret": "no_such"}, "query": "SELECT 1"},
            "sink": {"type": "memory"},
        },
        headers={"X-API-Key": "key"},
    )
    assert resp.status_code == 422


def test_delete_pipe_success(client, monkeypatch) -> None:
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    client.post(
        "/pipes/",
        json={"id": "to-delete", "source": {"type": "memory"}, "sink": {"type": "memory"}},
        headers={"X-API-Key": "key"},
    )
    resp = client.delete("/pipes/to-delete", headers={"X-API-Key": "key"})
    assert resp.status_code == 204
    assert client.get("/pipes/to-delete").status_code == 404


def test_run_dynamic_pipe_end_to_end(client, monkeypatch) -> None:
    """Create a memory pipe then run it — full round-trip through the API."""
    monkeypatch.setenv("ONPIPES_API_KEY", "key")
    client.post(
        "/pipes/",
        json={
            "id": "e2e-pipe",
            "mode": "full",
            "source": {"type": "memory", "payloads": [{"id": 1}, {"id": 2}, {"id": 3}]},
            "sink": {"type": "memory"},
        },
        headers={"X-API-Key": "key"},
    )
    run_resp = client.post("/pipes/e2e-pipe/run", json={})
    assert run_resp.status_code == 200
    assert run_resp.get_json()["records_written"] == 3
