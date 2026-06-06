from __future__ import annotations

import pytest

from openneuronic.pipes.broker._base import InMemoryBroker, _record_to_bytes, _bytes_to_record
from openneuronic.pipes.core.record import Record


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def test_record_round_trip_bytes() -> None:
    original = Record(payload={"id": 1, "name": "Alice"}, source_id="src", pipe_id="p")
    restored = _bytes_to_record(_record_to_bytes(original))
    assert restored.id == original.id
    assert restored.payload == original.payload
    assert restored.source_id == original.source_id
    assert restored.emitted_at == original.emitted_at


# ---------------------------------------------------------------------------
# InMemoryBroker — publish / consume
# ---------------------------------------------------------------------------


async def test_publish_and_consume_records() -> None:
    broker = InMemoryBroker()
    records = [Record(payload={"id": i}) for i in range(5)]

    await broker.publish("orders", records)
    assert broker.queue_depth("orders") == 5

    consumed: list[Record] = []
    async for r in broker.consume("orders", batch_size=10):
        consumed.append(r)

    assert len(consumed) == 5
    assert broker.queue_depth("orders") == 0


async def test_consume_respects_batch_size() -> None:
    broker = InMemoryBroker()
    await broker.publish("t", [Record(payload={"i": i}) for i in range(10)])

    consumed = [r async for r in broker.consume("t", batch_size=3)]
    assert len(consumed) == 3
    assert broker.queue_depth("t") == 7


async def test_consume_empty_topic_returns_nothing() -> None:
    broker = InMemoryBroker()
    consumed = [r async for r in broker.consume("nonexistent")]
    assert consumed == []


async def test_publish_to_multiple_topics_independent() -> None:
    broker = InMemoryBroker()
    await broker.publish("topic-a", [Record(payload={"x": 1})])
    await broker.publish("topic-b", [Record(payload={"x": 2})])

    a = [r async for r in broker.consume("topic-a")]
    b = [r async for r in broker.consume("topic-b")]
    assert len(a) == 1
    assert len(b) == 1
    assert a[0].payload != b[0].payload


# ---------------------------------------------------------------------------
# InMemoryBroker — DLQ
# ---------------------------------------------------------------------------


async def test_publish_dlq_stores_records() -> None:
    broker = InMemoryBroker()
    records = [Record(payload={"id": 1}), Record(payload={"id": 2})]
    await broker.publish_dlq("orders", records, reason="schema_mismatch")

    dlq = broker.dlq_records("orders")
    assert len(dlq) == 2
    assert all(r == "schema_mismatch" for _, r in dlq)


async def test_publish_dlq_separate_from_main_queue() -> None:
    broker = InMemoryBroker()
    await broker.publish("orders", [Record(payload={"id": 1})])
    await broker.publish_dlq("orders", [Record(payload={"id": 99})], reason="bad")

    consumed = [r async for r in broker.consume("orders")]
    assert len(consumed) == 1  # only the main-queue record
    assert consumed[0].payload["id"] == 1


# ---------------------------------------------------------------------------
# Setup / teardown are no-ops
# ---------------------------------------------------------------------------


async def test_setup_teardown_noop() -> None:
    broker = InMemoryBroker()
    await broker.setup()
    await broker.teardown()
    # Second teardown should not raise
    await broker.teardown()
