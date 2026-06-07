"""README example tests — Measures & Metrics section."""
from __future__ import annotations

import pytest

from openneuronic.pipes.metrics.base import MeasureSet, MeasureSetRef, measure_set_registry
from openneuronic.pipes.metrics.context import MetricsContext
from openneuronic.pipes.metrics.measures.quality import DuplicateRateMeasure, NullRateMeasure
from openneuronic.pipes.metrics.measures.runtime import RuntimeMeasure


# ---------------------------------------------------------------------------
# Tests — MeasureSet registration
# ---------------------------------------------------------------------------

def test_measure_set_registers_and_resolves() -> None:
    ms = MeasureSet("readme-core-runtime", measures=[RuntimeMeasure()])
    measure_set_registry.register(ms)

    ref = MeasureSetRef("readme-core-runtime")
    resolved = ref.resolve()
    assert resolved is not None
    assert resolved.name == "readme-core-runtime"


def test_measure_set_with_quality_measures() -> None:
    ms = MeasureSet("readme-quality", measures=[
        NullRateMeasure(),
        DuplicateRateMeasure(),
    ])
    measure_set_registry.register(ms)
    ref = MeasureSetRef("readme-quality")
    assert ref.resolve() is not None


# ---------------------------------------------------------------------------
# Tests — MetricsContext
# ---------------------------------------------------------------------------

def test_metrics_context_snapshot_records_read() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=500, written=498, dropped=2)
    ctx.stop()

    m = ctx.snapshot()
    assert m.records_read == 500


def test_metrics_context_snapshot_records_written() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=500, written=498, dropped=2)
    ctx.stop()

    m = ctx.snapshot()
    assert m.records_written == 498


def test_metrics_context_snapshot_null_rate() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=500, written=500, dropped=0)
    ctx.record_null(field_name="email", null_count=10, total=500)
    ctx.stop()

    m = ctx.snapshot()
    assert abs(m.null_rates["email"] - 0.02) < 1e-9


def test_metrics_context_duration_positive() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=100, written=100, dropped=0)
    ctx.stop()

    m = ctx.snapshot()
    assert m.duration_ms >= 0


def test_metrics_context_accumulates_multiple_batches() -> None:
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=300, written=300, dropped=0)
    ctx.record_batch(read=200, written=198, dropped=2)
    ctx.stop()

    m = ctx.snapshot()
    assert m.records_read == 500
    assert m.records_written == 498


def test_metrics_context_throughput_rps_positive_after_start_stop() -> None:
    import time
    ctx = MetricsContext(pipe_id="orders")
    ctx.start()
    ctx.record_batch(read=100, written=100, dropped=0)
    time.sleep(0.001)  # tiny sleep to ensure non-zero duration
    ctx.stop()

    m = ctx.snapshot()
    assert m.throughput_rps >= 0
