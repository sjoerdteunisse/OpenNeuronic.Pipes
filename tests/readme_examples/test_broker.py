"""README example tests — Broker section."""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import CopyMode, InMemoryBroker, Pipe, Record
from openneuronic.pipes.broker.runner import BrokerPipeRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int = 5) -> None:
        self._n = n

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _g():
            for i in range(n):
                yield Record(payload={"id": i, "val": f"v{i}"})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass

    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)

    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


def _pipe(n: int = 5) -> Pipe:
    return Pipe(id="broker-test", source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.INCREMENTAL)


# ---------------------------------------------------------------------------
# Tests — full pipeline run
# ---------------------------------------------------------------------------

async def test_broker_run_succeeds() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    result = await runner.run(_pipe(5))
    assert result.success


async def test_broker_run_records_published_count() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    result = await runner.run(_pipe(5))
    assert result.records_published == 5


async def test_broker_run_records_written_count() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    result = await runner.run(_pipe(5))
    assert result.records_written == 5


async def test_broker_run_result_has_started_at() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    result = await runner.run(_pipe(3))
    assert result.started_at is not None


# ---------------------------------------------------------------------------
# Tests — three-stage model
# ---------------------------------------------------------------------------

async def test_broker_read_to_broker_returns_count() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    published = await runner.read_to_broker(_pipe(4), topic="orders.raw")
    assert published == 4


async def test_broker_process_from_broker_returns_count() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    pipe = _pipe(4)
    await runner.read_to_broker(pipe, topic="orders.raw2")
    processed = await runner.process_from_broker(pipe, in_topic="orders.raw2", out_topic="orders.processed2")
    assert processed == 4


async def test_broker_write_from_broker_returns_count() -> None:
    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)
    pipe = _pipe(4)
    await runner.read_to_broker(pipe, topic="orders.raw3")
    await runner.process_from_broker(pipe, in_topic="orders.raw3", out_topic="orders.proc3")
    written = await runner.write_from_broker(pipe, topic="orders.proc3")
    assert written == 4
