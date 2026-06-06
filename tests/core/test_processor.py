from __future__ import annotations

import pytest

from openneuronic.pipes.core.record import Record
from openneuronic.pipes.processors.base import Processor


class PassthroughProcessor(Processor):
    async def process(self, record: Record) -> Record | None:
        return record


class FilterProcessor(Processor):
    """Keeps only records whose payload has ``keep=True``."""

    async def process(self, record: Record) -> Record | None:
        return record if record.payload.get("keep") else None


class MutateProcessor(Processor):
    """Adds a ``processed=True`` field to the payload."""

    async def process(self, record: Record) -> Record | None:
        record.payload["processed"] = True
        return record


async def test_passthrough_returns_all() -> None:
    proc = PassthroughProcessor()
    records = [Record(payload={"i": i}) for i in range(5)]
    result = await proc.process_batch(records)
    assert len(result) == 5


async def test_filter_drops_records() -> None:
    proc = FilterProcessor()
    records = [
        Record(payload={"keep": True}),
        Record(payload={"keep": False}),
        Record(payload={"keep": True}),
        Record(payload={}),
    ]
    result = await proc.process_batch(records)
    assert len(result) == 2
    assert all(r.payload.get("keep") is True for r in result)


async def test_mutate_modifies_payload() -> None:
    proc = MutateProcessor()
    records = [Record(payload={"val": i}) for i in range(3)]
    result = await proc.process_batch(records)
    assert len(result) == 3
    assert all(r.payload["processed"] is True for r in result)


async def test_setup_and_teardown_are_noop_by_default() -> None:
    proc = PassthroughProcessor()
    await proc.setup()
    await proc.teardown()


async def test_process_batch_empty_input() -> None:
    proc = PassthroughProcessor()
    result = await proc.process_batch([])
    assert result == []
