"""Broker abstraction layer — abstract base and in-memory implementation."""
from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from openneuronic.pipes.core.record import Record


# ---------------------------------------------------------------------------
# Serialisation helpers (shared by all broker implementations)
# ---------------------------------------------------------------------------

def _record_to_bytes(record: Record) -> bytes:
    d = dataclasses.asdict(record)
    d["emitted_at"] = d["emitted_at"].isoformat()
    return json.dumps(d).encode()


def _bytes_to_record(data: bytes) -> Record:
    d = json.loads(data)
    if isinstance(d.get("emitted_at"), str):
        d["emitted_at"] = datetime.datetime.fromisoformat(d["emitted_at"])
    return Record(**d)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class AbstractBroker(ABC):
    """Abstract message broker — publish / consume / DLQ interface."""

    @abstractmethod
    async def setup(self) -> None:
        """Connect and declare infrastructure."""

    @abstractmethod
    async def teardown(self) -> None:
        """Disconnect and release resources."""

    @abstractmethod
    async def publish(self, topic: str, records: list[Record]) -> None:
        """Publish *records* to *topic*."""

    @abstractmethod
    def consume(self, topic: str, *, batch_size: int = 100) -> AsyncIterator[Record]:
        """Yield up to *batch_size* records from *topic* and stop.

        Implementations should return an async generator.  Callers drive
        consumption explicitly — this is not a long-running subscription.
        """

    @abstractmethod
    async def publish_dlq(
        self, topic: str, records: list[Record], reason: str
    ) -> None:
        """Route failed *records* to the dead-letter queue for *topic*."""


# ---------------------------------------------------------------------------
# In-memory broker (for tests and local development without a broker)
# ---------------------------------------------------------------------------

class InMemoryBroker(AbstractBroker):
    """Thread-unsafe in-process broker backed by :class:`asyncio.Queue`.

    Useful for unit tests and single-process development without a real
    message broker.  DLQ entries are kept in a plain list.
    """

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[Record]] = {}
        self._dlq: dict[str, list[tuple[Record, str]]] = {}

    async def setup(self) -> None:  # no-op
        pass

    async def teardown(self) -> None:  # no-op
        pass

    async def publish(self, topic: str, records: list[Record]) -> None:
        q = self._queues.setdefault(topic, asyncio.Queue())
        for r in records:
            await q.put(r)

    def consume(self, topic: str, *, batch_size: int = 100) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            q = self._queues.setdefault(topic, asyncio.Queue())
            count = 0
            while count < batch_size:
                try:
                    yield q.get_nowait()
                    count += 1
                except asyncio.QueueEmpty:
                    break

        return _gen()

    async def publish_dlq(
        self, topic: str, records: list[Record], reason: str
    ) -> None:
        self._dlq.setdefault(topic, []).extend((r, reason) for r in records)

    def dlq_records(self, topic: str) -> list[tuple[Record, str]]:
        """Return all DLQ entries for *topic* (for test assertions)."""
        return list(self._dlq.get(topic, []))

    def queue_depth(self, topic: str) -> int:
        """Return the number of unconsumed records in *topic*."""
        return self._queues[topic].qsize() if topic in self._queues else 0
