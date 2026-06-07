"""Equivalence integration tests — Opus Orchestration.

Asserts that:
- OpusRunner total records_written == sum of individual LocalRunner runs for each segment.
- Dynamic fan-out produces N segments and runs all of them.

Run with::

    py -3.14 -m pytest tests/integration/test_opus_equivalence.py -v -m integration
"""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    LocalRunner,
    Opus,
    OpusRunner,
    Pipe,
    PipeSegment,
    Record,
)
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
                yield Record(payload={"id": i})
        return _g()

    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _CaptureSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []
        self._table = "items"
        self._auto_migrate = False

    async def setup(self) -> None: pass
    async def write(self, records: list[Record]) -> None: self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


def _pipe(n: int, pipe_id: str) -> Pipe:
    return Pipe(id=pipe_id, source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.INCREMENTAL)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_opus_total_written_equals_sum_of_individual_runs() -> None:
    """OpusRunner total == sum of individual LocalRunner runs for same source sizes."""
    configs = [("seg-a", 10), ("seg-b", 15), ("seg-c", 20)]

    # Individual LocalRunner totals
    individual_total = 0
    for pipe_id, n in configs:
        result = await LocalRunner().run(_pipe(n, pipe_id + "-single"))
        individual_total += result.records_written

    # OpusRunner total
    opus = Opus(id="equiv-opus", durable=False)
    opus.add_segments([
        PipeSegment(id=pid, pipe=_pipe(n, pid + "-opus"))
        for pid, n in configs
    ])
    opus_result = await OpusRunner().run(opus)
    opus_total = sum(sr.run_result.records_written for sr in opus_result.segment_results.values())

    assert individual_total == opus_total


async def test_opus_segment_results_include_all_segments() -> None:
    """segment_results must have an entry for every segment in the opus."""
    ids = ["alpha", "beta", "gamma"]
    opus = Opus(id="all-segments", durable=False)
    opus.add_segments([PipeSegment(id=sid, pipe=_pipe(5, sid)) for sid in ids])

    result = await OpusRunner().run(opus)
    for sid in ids:
        assert sid in result.segment_results


async def test_opus_success_requires_all_segments_succeed() -> None:
    """Opus result.success is True only when all segments succeed."""
    opus = Opus(id="all-ok", durable=False)
    opus.add_segments([
        PipeSegment(id="s1", pipe=_pipe(3, "s1")),
        PipeSegment(id="s2", pipe=_pipe(4, "s2"), depends_on=["s1"]),
    ])
    result = await OpusRunner().run(opus)
    assert result.success
    for sr in result.segment_results.values():
        assert sr.run_result.success


async def test_dynamic_fan_out_runs_all_generated_segments() -> None:
    """Dynamic fan-out creates N segments and all run successfully."""
    keys = ["t1", "t2", "t3", "t4"]
    opus = Opus(id="fan-out-equiv", durable=False)
    opus.add_segments([PipeSegment(id="root", pipe=_pipe(2, "root"))])
    opus.add_dynamic_segments(
        segment_factory=lambda k: PipeSegment(id=f"load-{k}", pipe=_pipe(3, f"load-{k}")),
        keys=keys,
        depends_on=["root"],
        id_prefix="dyn",
    )

    result = await OpusRunner().run(opus)
    assert result.success
    # 1 root + 4 dynamic
    assert len(result.segment_results) == 5


async def test_opus_chain_records_written_equals_independent_sum() -> None:
    """Chain A→B→C: total written == n_a + n_b + n_c."""
    n_a, n_b, n_c = 5, 7, 9

    opus = Opus(id="chain-equiv", durable=False)
    opus.add_segments([
        PipeSegment(id="a", pipe=_pipe(n_a, "a")),
        PipeSegment(id="b", pipe=_pipe(n_b, "b"), depends_on=["a"]),
        PipeSegment(id="c", pipe=_pipe(n_c, "c"), depends_on=["b"]),
    ])
    result = await OpusRunner().run(opus)

    total = sum(sr.run_result.records_written for sr in result.segment_results.values())
    assert total == n_a + n_b + n_c
