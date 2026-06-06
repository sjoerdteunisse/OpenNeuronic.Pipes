from __future__ import annotations

import copy

import pytest

from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.metrics.base import (
    Measure,
    MeasureSet,
    MeasureSetRef,
    RunMetrics,
    measure_set_registry,
)
from openneuronic.pipes.metrics.context import MetricsContext
from openneuronic.pipes.metrics.measures.runtime import RuntimeMeasure
from openneuronic.pipes.runner import LocalRunner
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource
from openneuronic.pipes.core.bookmark import Bookmark
from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# MetricsContext
# ---------------------------------------------------------------------------


def test_context_accumulates_across_batches() -> None:
    ctx = MetricsContext(pipe_id="test")
    ctx.start()
    ctx.record_batch(read=100, written=98, dropped=2)
    ctx.record_batch(read=50, written=50, dropped=0)
    ctx.stop()

    assert ctx.records_read == 150
    assert ctx.records_written == 148
    assert ctx.records_dropped == 2
    assert ctx.batches == 2
    assert ctx.duration_ms > 0


def test_context_snapshot_produces_run_metrics() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=10, written=10, dropped=0)
    ctx.stop()

    m = ctx.snapshot()
    assert isinstance(m, RunMetrics)
    assert m.pipe_id == "orders"
    assert m.records_read == 10
    assert m.records_written == 10
    assert m.duration_ms > 0
    assert m.throughput_rps > 0


def test_context_error_counter() -> None:
    ctx = MetricsContext(pipe_id="p")
    ctx.start()
    ctx.record_error()
    ctx.record_error()
    ctx.stop()

    m = ctx.snapshot()
    assert m.errors == 2


def test_context_null_rate() -> None:
    ctx = MetricsContext(pipe_id="p")
    ctx.start()
    ctx.record_null("price", null_count=2, total=10)
    ctx.record_null("price", null_count=1, total=10)
    ctx.stop()

    m = ctx.snapshot()
    assert "price" in m.null_rates
    assert abs(m.null_rates["price"] - 0.15) < 0.01  # (2+1)/(10+10) = 0.15


def test_context_row_count_deviation() -> None:
    ctx = MetricsContext(pipe_id="p")
    ctx.start()
    ctx.set_expected_row_count(1000)
    ctx.record_batch(read=900, written=900, dropped=0)
    ctx.stop()

    m = ctx.snapshot()
    assert m.row_count_deviation is not None
    assert abs(m.row_count_deviation - 0.1) < 0.001  # |900-1000|/1000 = 0.1


def test_context_throughput_zero_duration() -> None:
    ctx = MetricsContext(pipe_id="p")
    # No start/stop — duration_ms is 0
    assert ctx.throughput_rps == 0.0


# ---------------------------------------------------------------------------
# RuntimeMeasure
# ---------------------------------------------------------------------------


def test_runtime_measure_collects_keys() -> None:
    ctx = MetricsContext(pipe_id="p")
    ctx.start()
    ctx.record_batch(read=5, written=4, dropped=1)
    ctx.stop()

    result = RuntimeMeasure().collect(ctx)
    assert result["records_read"] == 5
    assert result["records_written"] == 4
    assert result["records_dropped"] == 1
    assert result["batches"] == 1
    assert result["duration_ms"] > 0
    assert result["throughput_rps"] > 0


# ---------------------------------------------------------------------------
# MeasureSet + registry
# ---------------------------------------------------------------------------


def test_measure_set_collects_all_measures() -> None:
    ctx = MetricsContext(pipe_id="p")
    ctx.start()
    ctx.record_batch(read=10, written=10, dropped=0)
    ctx.stop()

    ms = MeasureSet("core-runtime", [RuntimeMeasure()])
    result = ms.collect_all(ctx)
    assert "records_read" in result
    assert "throughput_rps" in result


def test_measure_set_ref_resolves() -> None:
    ms = MeasureSet("my-set", [RuntimeMeasure()])
    measure_set_registry.register(ms)
    ref = MeasureSetRef("my-set")
    assert ref.resolve() is ms


def test_measure_set_ref_missing_raises() -> None:
    ref = MeasureSetRef("nonexistent-set")
    with pytest.raises(KeyError, match="nonexistent-set"):
        ref.resolve()


# ---------------------------------------------------------------------------
# LocalRunner metrics opt-in
# ---------------------------------------------------------------------------


class _StubSource(AbstractSource):
    def __init__(self, n: int = 5) -> None:
        self._n = n

    async def setup(self) -> None: pass

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        async def _gen():
            for i in range(self._n):
                yield Record(payload={"id": i})
        return _gen()

    async def teardown(self) -> None: pass


class _StubSink(AbstractSink):
    async def setup(self) -> None: pass
    async def write(self, records): pass
    async def commit_bookmark(self, bookmark): pass
    async def teardown(self) -> None: pass


async def test_runner_no_measures_metrics_is_none() -> None:
    pipe = Pipe(id="p", source=_StubSource(), sink=_StubSink(), mode=CopyMode.FULL)
    result = await LocalRunner().run(pipe)
    assert result.metrics is None


async def test_runner_with_measure_ref_produces_metrics() -> None:
    ms = MeasureSet("runner-test-set", [RuntimeMeasure()])
    measure_set_registry.register(ms)

    pipe = Pipe(
        id="p",
        source=_StubSource(10),
        sink=_StubSink(),
        mode=CopyMode.FULL,
        measures=[MeasureSetRef("runner-test-set")],
    )
    result = await LocalRunner(batch_size=5).run(pipe)

    assert result.metrics is not None
    assert result.metrics.records_read == 10
    assert result.metrics.records_written == 10
    assert result.metrics.batches == 2
    assert result.metrics.duration_ms > 0
    assert result.metrics.throughput_rps > 0
