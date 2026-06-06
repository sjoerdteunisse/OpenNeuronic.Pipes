from __future__ import annotations

import datetime

from openneuronic.pipes.core.record import Record


def test_record_defaults() -> None:
    r = Record()
    assert isinstance(r.id, str) and len(r.id) == 36  # UUID4 format
    assert r.payload == {}
    assert r.metadata == {}
    assert r.source_id == ""
    assert r.pipe_id == ""
    assert r.schema_version == 1
    assert r.contract_version == 1
    assert isinstance(r.emitted_at, datetime.datetime)
    assert r.emitted_at.tzinfo is not None  # UTC-aware


def test_record_unique_ids() -> None:
    ids = {Record().id for _ in range(100)}
    assert len(ids) == 100


def test_record_custom_payload() -> None:
    r = Record(
        payload={"id": 42, "name": "test"},
        metadata={"trace_id": "abc"},
        source_id="my-source",
        pipe_id="my-pipe",
        schema_version=3,
        contract_version=2,
    )
    assert r.payload["id"] == 42
    assert r.metadata["trace_id"] == "abc"
    assert r.source_id == "my-source"
    assert r.schema_version == 3
    assert r.contract_version == 2


def test_record_payloads_are_independent() -> None:
    r1 = Record()
    r2 = Record()
    r1.payload["x"] = 1
    assert "x" not in r2.payload
