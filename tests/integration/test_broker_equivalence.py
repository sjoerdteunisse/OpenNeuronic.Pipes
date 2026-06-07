"""Equivalence integration tests — Broker vs LocalRunner.

Asserts that running a pipe via BrokerPipeRunner (in-memory broker, 3-stage)
produces the same records_written count as running the same pipe via LocalRunner.

These tests use only in-memory stubs so they run without any external services,
but they are marked ``integration`` to keep them separate from fast unit tests.

Run with::

    py -3.14 -m pytest tests/integration/test_broker_equivalence.py -v -m integration
"""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    InMemoryBroker,
    LocalRunner,
    Pipe,
    Record,
)
from openneuronic.pipes.broker.runner import BrokerPipeRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int) -> None:
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_broker_vs_local_runner_records_written_equivalence() -> None:
    """BrokerPipeRunner records_written must equal LocalRunner records_written."""
    n = 50

    sink_local = _CaptureSink()
    pipe_local = Pipe(id="equiv-local", source=_CountSource(n), sink=sink_local, mode=CopyMode.INCREMENTAL)

    sink_broker = _CaptureSink()
    pipe_broker = Pipe(id="equiv-broker", source=_CountSource(n), sink=sink_broker, mode=CopyMode.INCREMENTAL)

    result_local = await LocalRunner().run(pipe_local)
    result_broker = await BrokerPipeRunner(InMemoryBroker(), batch_size=500).run(pipe_broker)

    assert result_local.records_written == result_broker.records_written, (
        f"LocalRunner wrote {result_local.records_written}, "
        f"BrokerPipeRunner wrote {result_broker.records_written}"
    )


async def test_broker_vs_local_runner_payload_equivalence() -> None:
    """All payloads written by LocalRunner and BrokerPipeRunner are identical."""
    n = 20

    sink_local = _CaptureSink()
    pipe_local = Pipe(id="payload-local", source=_CountSource(n), sink=sink_local, mode=CopyMode.INCREMENTAL)

    sink_broker = _CaptureSink()
    pipe_broker = Pipe(id="payload-broker", source=_CountSource(n), sink=sink_broker, mode=CopyMode.INCREMENTAL)

    await LocalRunner().run(pipe_local)
    await BrokerPipeRunner(InMemoryBroker(), batch_size=500).run(pipe_broker)

    ids_local  = {r.payload["id"] for r in sink_local.written}
    ids_broker = {r.payload["id"] for r in sink_broker.written}
    assert ids_local == ids_broker


async def test_broker_three_stage_vs_single_run_equivalence() -> None:
    """Three individual stage calls produce same count as a single BrokerPipeRunner.run()."""
    n = 30

    broker = InMemoryBroker()
    runner = BrokerPipeRunner(broker=broker, batch_size=500)

    sink_staged = _CaptureSink()
    pipe_staged = Pipe(id="staged", source=_CountSource(n), sink=sink_staged, mode=CopyMode.INCREMENTAL)

    published  = await runner.read_to_broker(pipe_staged, topic="equiv.raw")
    processed  = await runner.process_from_broker(pipe_staged, in_topic="equiv.raw", out_topic="equiv.proc")
    written    = await runner.write_from_broker(pipe_staged, topic="equiv.proc")

    # Single-run path
    broker2 = InMemoryBroker()
    runner2 = BrokerPipeRunner(broker=broker2, batch_size=500)
    sink_single = _CaptureSink()
    pipe_single = Pipe(id="single", source=_CountSource(n), sink=sink_single, mode=CopyMode.INCREMENTAL)
    result_single = await runner2.run(pipe_single)

    assert written == result_single.records_written
    assert published == n
