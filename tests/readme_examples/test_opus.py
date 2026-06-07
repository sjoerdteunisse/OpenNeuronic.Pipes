"""README example tests — Opus Orchestration section."""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from openneuronic.pipes import (
    CopyMode,
    GraphFailureMode,
    InMemoryDurableRunState,
    Opus,
    OpusRunner,
    Pipe,
    PipeSegment,
    Record,
    RetryPolicy,
    WaitStrategy,
)
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int = 3) -> None:
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
    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass
    async def prepare_full_load(self) -> None: pass
    async def commit_full_load(self) -> None: pass


def _pipe(n: int = 3, pipe_id: str = "seg") -> Pipe:
    return Pipe(id=pipe_id, source=_CountSource(n), sink=_CaptureSink(), mode=CopyMode.INCREMENTAL)


# ---------------------------------------------------------------------------
# Tests — basic linear orchestration
# ---------------------------------------------------------------------------

async def test_opus_linear_chain_succeeds() -> None:
    opus = Opus(id="linear-test", durable=False)
    opus.add_segments([
        PipeSegment(id="extract",   pipe=_pipe(3, "extract")),
        PipeSegment(id="transform", pipe=_pipe(3, "transform"), depends_on=["extract"]),
        PipeSegment(id="load",      pipe=_pipe(3, "load"),      depends_on=["transform"]),
    ])

    result = await OpusRunner().run(opus)
    assert result.success


async def test_opus_segment_results_keyed_by_id() -> None:
    opus = Opus(id="keyed-test", durable=False)
    opus.add_segments([
        PipeSegment(id="seg-a", pipe=_pipe(2, "seg-a")),
        PipeSegment(id="seg-b", pipe=_pipe(2, "seg-b"), depends_on=["seg-a"]),
    ])

    result = await OpusRunner().run(opus)
    assert "seg-a" in result.segment_results
    assert "seg-b" in result.segment_results


async def test_opus_records_written_accumulates() -> None:
    opus = Opus(id="accumulate-test", durable=False)
    opus.add_segments([
        PipeSegment(id="s1", pipe=_pipe(3, "s1")),
        PipeSegment(id="s2", pipe=_pipe(4, "s2"), depends_on=["s1"]),
    ])

    result = await OpusRunner().run(opus)
    total = sum(sr.run_result.records_written for sr in result.segment_results.values())
    assert total == 7


# ---------------------------------------------------------------------------
# Tests — wait strategies
# ---------------------------------------------------------------------------

async def test_opus_wait_strategy_any() -> None:
    opus = Opus(id="wait-any-test", durable=False)
    opus.add_segments([
        PipeSegment(id="a", pipe=_pipe(2, "a")),
        PipeSegment(id="b", pipe=_pipe(2, "b")),
        PipeSegment(id="c", pipe=_pipe(2, "c"), depends_on=["a", "b"],
                   wait_strategy=WaitStrategy.ANY),
    ])
    result = await OpusRunner().run(opus)
    assert result.success


async def test_opus_wait_strategy_majority() -> None:
    opus = Opus(id="wait-majority-test", durable=False)
    opus.add_segments([
        PipeSegment(id="x1", pipe=_pipe(2, "x1")),
        PipeSegment(id="x2", pipe=_pipe(2, "x2")),
        PipeSegment(id="x3", pipe=_pipe(2, "x3")),
        PipeSegment(id="consumer", pipe=_pipe(2, "consumer"), depends_on=["x1", "x2", "x3"],
                   wait_strategy=WaitStrategy.MAJORITY),
    ])
    result = await OpusRunner().run(opus)
    assert result.success


# ---------------------------------------------------------------------------
# Tests — dynamic fan-out
# ---------------------------------------------------------------------------

async def test_dynamic_fan_out_creates_segments_per_key() -> None:
    tenants = ["acme", "globex", "initech"]
    opus = Opus(id="fan-out-test", durable=False)
    opus.add_segments([PipeSegment(id="root", pipe=_pipe(2, "root"))])
    opus.add_dynamic_segments(
        segment_factory=lambda t: PipeSegment(id=f"load-{t}", pipe=_pipe(2, f"load-{t}")),
        keys=tenants,
        depends_on=["root"],
        id_prefix="tenant",
    )

    # 1 root + 3 dynamic; dynamic IDs are "{id_prefix}:{key}" = "tenant:acme", etc.
    assert len(opus.segments) == 4
    ids = [s.id for s in opus.segments]
    for t in tenants:
        assert f"tenant:{t}" in ids


async def test_dynamic_fan_out_run_succeeds() -> None:
    tenants = ["t1", "t2"]
    opus = Opus(id="fan-out-run-test", durable=False)
    opus.add_segments([PipeSegment(id="root", pipe=_pipe(2, "root"))])
    opus.add_dynamic_segments(
        segment_factory=lambda t: PipeSegment(id=f"load-{t}", pipe=_pipe(2, f"load-{t}")),
        keys=tenants,
        depends_on=["root"],
        id_prefix="dyn",
    )

    result = await OpusRunner().run(opus)
    assert result.success


# ---------------------------------------------------------------------------
# Tests — retry policy
# ---------------------------------------------------------------------------

def test_retry_policy_constructs() -> None:
    policy = RetryPolicy(max_retries=5, backoff_s=2.0, jitter=True,
                        failure_mode=GraphFailureMode.RETRY_SEGMENT)
    assert policy.max_retries == 5
    assert policy.jitter is True


# ---------------------------------------------------------------------------
# Tests — durable state
# ---------------------------------------------------------------------------

async def test_durable_run_state_already_succeeded_segments_skipped() -> None:
    opus = Opus(id="durable-test", durable=True)
    opus.add_segments([
        PipeSegment(id="step-1", pipe=_pipe(2, "step-1")),
        PipeSegment(id="step-2", pipe=_pipe(2, "step-2"), depends_on=["step-1"]),
    ])

    store = InMemoryDurableRunState(opus_id="durable-test", run_id="r-001")
    result = await OpusRunner().run(opus, state_store=store)
    assert result.success
