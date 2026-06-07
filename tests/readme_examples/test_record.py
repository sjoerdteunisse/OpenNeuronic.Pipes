"""README example tests — Records section."""
from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes import Record


def test_records_get_unique_ids() -> None:
    r1 = Record(payload={"id": 1, "name": "Alice"})
    r2 = Record(payload={"id": 2, "name": "Bob"})
    assert r1.id != r2.id


def test_record_default_payload_is_empty_dict() -> None:
    r = Record()
    assert r.payload == {}


def test_record_version_and_metadata_fields() -> None:
    r = Record(
        payload={"order_id": "abc"},
        metadata={"trace_id": "xyz-123"},
        schema_version=2,
        contract_version=1,
    )
    assert r.schema_version == 2
    assert r.contract_version == 1
    assert r.metadata["trace_id"] == "xyz-123"


def test_record_emitted_at_is_utc_aware() -> None:
    r = Record(payload={"x": 1})
    assert r.emitted_at.tzinfo is not None
    assert r.emitted_at.utcoffset() == datetime.timedelta(0)


def test_record_pipe_id_defaults_empty() -> None:
    r = Record()
    assert r.pipe_id == ""


def test_record_source_id_defaults_empty() -> None:
    r = Record()
    assert r.source_id == ""


def test_record_schema_version_defaults_to_one() -> None:
    r = Record()
    assert r.schema_version == 1


def test_record_contract_version_defaults_to_one() -> None:
    r = Record()
    assert r.contract_version == 1


def test_record_payload_stored_as_provided() -> None:
    payload = {"key": "value", "num": 42}
    r = Record(payload=payload)
    assert r.payload["key"] == "value"
    assert r.payload["num"] == 42
