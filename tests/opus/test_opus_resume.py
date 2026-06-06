"""Tests for Opus crash-resume semantics using InMemoryDurableRunState."""
from __future__ import annotations

from collections.abc import AsyncIterator

from openneuronic.pipes.core.enums import CopyMode, GraphFailureMode, SegmentStatus
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.opus.durability import filter_pending_segments
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.retry import RetryPolicy
from openneuronic.pipes.opus.runner import OpusRunner
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.opus.state import InMemoryDurableRunState, SegmentState
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


class _CountSource(AbstractSource):
    source_id = "stub"

    def __init__(self, n: int = 2) -> None:
        self._n = n

    async def setup(self) -> None: pass

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _g():
            for i in range(n):
                yield Record(payload={"id": i})
        return _g()

    async def teardown(self) -> None: pass


class _TrackSink(AbstractSink):
    def __init__(self) -> None:
        self.call_count = 0

    async def setup(self) -> None: pass

    async def write(self, records: list[Record]) -> None:
        self.call_count += 1

    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass


def _pipe(name: str, sink: AbstractSink | None = None) -> Pipe:
    return Pipe(
        id=name,
        source=_CountSource(),
        sink=sink or _TrackSink(),
        mode=CopyMode.FULL,
    )


# ---------------------------------------------------------------------------
# filter_pending_segments
# ---------------------------------------------------------------------------


async def test_filter_pending_skips_success_segments() -> None:
    opus = Opus(id="o", durable=True)
    opus.add_segments([
        PipeSegment(id="done",    pipe=_pipe("done")),
        PipeSegment(id="pending", pipe=_pipe("pending")),
    ])

    store = InMemoryDurableRunState("o", "run-1")
    await store.set_segment_state(SegmentState("done", status=SegmentStatus.SUCCESS))

    pending = await filter_pending_segments(opus, store)
    assert len(pending) == 1
    assert pending[0].id == "pending"


async def test_filter_pending_includes_failed_segments() -> None:
    opus = Opus(id="o", durable=True)
    opus.add_segments([PipeSegment(id="failed", pipe=_pipe("failed"))])

    store = InMemoryDurableRunState("o", "run-1")
    await store.set_segment_state(SegmentState("failed", status=SegmentStatus.FAILED))

    pending = await filter_pending_segments(opus, store)
    assert len(pending) == 1
    assert pending[0].id == "failed"


async def test_filter_pending_all_success_returns_empty() -> None:
    opus = Opus(id="o", durable=True)
    opus.add_segments([
        PipeSegment(id="a", pipe=_pipe("a")),
        PipeSegment(id="b", pipe=_pipe("b")),
    ])

    store = InMemoryDurableRunState("o", "run-1")
    await store.set_segment_state(SegmentState("a", status=SegmentStatus.SUCCESS))
    await store.set_segment_state(SegmentState("b", status=SegmentStatus.SUCCESS))

    pending = await filter_pending_segments(opus, store)
    assert pending == []


# ---------------------------------------------------------------------------
# OpusRunner resume — re-runs only failed segments
# ---------------------------------------------------------------------------


async def test_resume_skips_already_successful_segment() -> None:
    sink_a = _TrackSink()
    sink_b = _TrackSink()

    opus = Opus(id="o", durable=True)
    opus.add_segments([
        PipeSegment(id="a", pipe=Pipe(id="a", source=_CountSource(), sink=sink_a, mode=CopyMode.FULL)),
        PipeSegment(id="b", pipe=Pipe(id="b", source=_CountSource(), sink=sink_b, mode=CopyMode.FULL)),
    ])

    store = InMemoryDurableRunState("o", "run-resume")
    # Mark "a" as already successful (simulates a crashed run where "a" had committed)
    await store.set_segment_state(SegmentState("a", status=SegmentStatus.SUCCESS))

    result = await OpusRunner(default_retry_policy=RetryPolicy(max_retries=0)).run(
        opus, state_store=store
    )

    # "b" ran, "a" was skipped
    assert sink_b.call_count > 0
    assert sink_a.call_count == 0  # skipped
    assert result.segment_results.get("a") is None  # never entered the runner


async def test_second_run_completes_after_first_failure() -> None:
    """Simulate full resume cycle: first run fails one segment, second run picks up."""
    sink_b = _TrackSink()

    class _FailFirst(AbstractSink):
        """Fails on the first call, succeeds thereafter."""
        def __init__(self) -> None:
            self._calls = 0
        async def setup(self) -> None: pass
        async def write(self, records):
            self._calls += 1
            if self._calls == 1:
                raise RuntimeError("first fail")
        async def commit_bookmark(self, b): pass
        async def teardown(self) -> None: pass

    bad_sink = _FailFirst()

    opus = Opus(id="o", durable=True, on_failure=GraphFailureMode.CONTINUE_INDEPENDENT_BRANCHES)
    opus.add_segments([
        PipeSegment(
            id="bad",
            pipe=Pipe(id="bad", source=_CountSource(), sink=bad_sink, mode=CopyMode.FULL),
            retry_policy=RetryPolicy(max_retries=0),
        ),
        PipeSegment(
            id="good",
            pipe=Pipe(id="good", source=_CountSource(), sink=sink_b, mode=CopyMode.FULL),
        ),
    ])

    store = InMemoryDurableRunState("o", "run-1")

    # First run — "bad" fails, "good" succeeds
    first = await OpusRunner(default_retry_policy=RetryPolicy(max_retries=0)).run(
        opus, state_store=store
    )
    assert not first.success
    good_state = await store.get_segment_state("good")
    assert good_state is not None and good_state.status == SegmentStatus.SUCCESS

    # Second run against same store — "good" is skipped, "bad" is retried
    second = await OpusRunner(default_retry_policy=RetryPolicy(max_retries=0)).run(
        opus, state_store=store
    )
    assert second.success
    # sink_b was only called once across both runs (skipped on second)
    assert sink_b.call_count == 1
