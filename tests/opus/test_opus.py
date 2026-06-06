from __future__ import annotations

import pytest

from collections.abc import AsyncIterator

from openneuronic.pipes.core.enums import CopyMode, GraphFailureMode, SegmentStatus, WaitStrategy
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.retry import RetryPolicy, run_with_retry
from openneuronic.pipes.opus.runner import OpusRunner
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.opus.state import InMemoryDurableRunState, SegmentState
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


# ---------------------------------------------------------------------------
# Stub source / sink
# ---------------------------------------------------------------------------


class _CountSource(AbstractSource):
    def __init__(self, n: int = 3, source_id: str = "stub") -> None:
        self.source_id = source_id
        self._n = n

    async def setup(self) -> None: pass

    def read(self, bookmark=None) -> AsyncIterator[Record]:
        n = self._n
        async def _gen():
            for i in range(n):
                yield Record(payload={"id": i})
        return _gen()

    async def teardown(self) -> None: pass


class _ListSink(AbstractSink):
    def __init__(self) -> None:
        self.written: list[Record] = []

    async def setup(self) -> None: pass

    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)

    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass


class _FailSink(AbstractSink):
    async def setup(self) -> None: pass
    async def write(self, records):
        raise RuntimeError("intentional failure")
    async def commit_bookmark(self, b): pass
    async def teardown(self) -> None: pass


def _make_pipe(n: int = 3, sink: AbstractSink | None = None) -> Pipe:
    return Pipe(
        id=f"pipe-{n}",
        source=_CountSource(n),
        sink=sink or _ListSink(),
        mode=CopyMode.FULL,
    )


# ---------------------------------------------------------------------------
# Opus DAG validation
# ---------------------------------------------------------------------------


def test_opus_rejects_unknown_dependency() -> None:
    opus = Opus(id="o")
    with pytest.raises(ValueError, match="unknown segment"):
        opus.add_segments([
            PipeSegment(id="b", pipe=_make_pipe(), depends_on=["nonexistent"]),
        ])


def test_opus_rejects_cycle() -> None:
    opus = Opus(id="o")
    with pytest.raises(ValueError, match="cycle"):
        opus.add_segments([
            PipeSegment(id="a", pipe=_make_pipe(), depends_on=["b"]),
            PipeSegment(id="b", pipe=_make_pipe(), depends_on=["a"]),
        ])


def test_opus_rejects_duplicate_id() -> None:
    opus = Opus(id="o")
    with pytest.raises(ValueError, match="already registered"):
        opus.add_segments([
            PipeSegment(id="x", pipe=_make_pipe()),
            PipeSegment(id="x", pipe=_make_pipe()),
        ])


# ---------------------------------------------------------------------------
# Topological waves
# ---------------------------------------------------------------------------


def test_single_segment_one_wave() -> None:
    opus = Opus(id="o")
    opus.add_segments([PipeSegment(id="a", pipe=_make_pipe())])
    waves = opus.topological_waves()
    assert len(waves) == 1
    assert waves[0][0].id == "a"


def test_linear_chain_produces_sequential_waves() -> None:
    opus = Opus(id="o")
    opus.add_segments([
        PipeSegment(id="a", pipe=_make_pipe()),
        PipeSegment(id="b", pipe=_make_pipe(), depends_on=["a"]),
        PipeSegment(id="c", pipe=_make_pipe(), depends_on=["b"]),
    ])
    waves = opus.topological_waves()
    assert len(waves) == 3
    assert [w[0].id for w in waves] == ["a", "b", "c"]


def test_parallel_segments_in_same_wave() -> None:
    opus = Opus(id="o")
    opus.add_segments([
        PipeSegment(id="root", pipe=_make_pipe()),
        PipeSegment(id="left",  pipe=_make_pipe(), depends_on=["root"]),
        PipeSegment(id="right", pipe=_make_pipe(), depends_on=["root"]),
        PipeSegment(id="merge", pipe=_make_pipe(), depends_on=["left", "right"]),
    ])
    waves = opus.topological_waves()
    assert len(waves) == 3
    parallel_ids = {s.id for s in waves[1]}
    assert parallel_ids == {"left", "right"}
    assert waves[2][0].id == "merge"


def test_wait_strategy_none_lands_in_wave_zero() -> None:
    opus = Opus(id="o")
    opus.add_segments([
        PipeSegment(id="always-on", pipe=_make_pipe(), wait_strategy=WaitStrategy.NONE),
        PipeSegment(id="normal",    pipe=_make_pipe()),
    ])
    waves = opus.topological_waves()
    first_wave_ids = {s.id for s in waves[0]}
    assert "always-on" in first_wave_ids


# ---------------------------------------------------------------------------
# OpusRunner — execution
# ---------------------------------------------------------------------------


async def test_opus_runner_sequential() -> None:
    execution_order: list[str] = []

    class _TrackingSink(AbstractSink):
        def __init__(self, name: str) -> None:
            self._name = name
        async def setup(self) -> None: pass
        async def write(self, records):
            execution_order.append(self._name)
        async def commit_bookmark(self, b): pass
        async def teardown(self) -> None: pass

    opus = Opus(id="seq")
    opus.add_segments([
        PipeSegment(id="first",  pipe=Pipe(id="p1", source=_CountSource(1), sink=_TrackingSink("first"),  mode=CopyMode.FULL)),
        PipeSegment(id="second", pipe=Pipe(id="p2", source=_CountSource(1), sink=_TrackingSink("second"), mode=CopyMode.FULL), depends_on=["first"]),
    ])

    result = await OpusRunner().run(opus)

    assert result.success
    assert execution_order == ["first", "second"]


async def test_opus_runner_all_segments_reported() -> None:
    opus = Opus(id="o")
    opus.add_segments([
        PipeSegment(id="a", pipe=_make_pipe()),
        PipeSegment(id="b", pipe=_make_pipe()),
    ])
    result = await OpusRunner().run(opus)
    assert "a" in result.segment_results
    assert "b" in result.segment_results
    assert result.success


async def test_opus_runner_fail_fast_stops_on_failure() -> None:
    opus = Opus(id="o", on_failure=GraphFailureMode.FAIL_FAST)
    opus.add_segments([
        PipeSegment(
            id="bad",
            pipe=Pipe(id="bad", source=_CountSource(1), sink=_FailSink(), mode=CopyMode.FULL),
            retry_policy=RetryPolicy(max_retries=0),
        ),
    ])

    with pytest.raises(RuntimeError, match="FAIL_FAST"):
        await OpusRunner().run(opus)


async def test_opus_runner_continue_independent_on_failure() -> None:
    """CONTINUE_INDEPENDENT_BRANCHES: a failing wave does not abort subsequent waves."""
    opus = Opus(id="o", on_failure=GraphFailureMode.CONTINUE_INDEPENDENT_BRANCHES)
    sink_b = _ListSink()
    opus.add_segments([
        PipeSegment(
            id="bad",
            pipe=Pipe(id="bad", source=_CountSource(1), sink=_FailSink(), mode=CopyMode.FULL),
            retry_policy=RetryPolicy(max_retries=0),
        ),
        PipeSegment(id="good", pipe=Pipe(id="good", source=_CountSource(2), sink=sink_b, mode=CopyMode.FULL)),
    ])

    result = await OpusRunner().run(opus)
    # "good" ran (independent branch)
    assert len(sink_b.written) == 2
    # Overall result is not success (bad segment failed)
    assert not result.success
    assert "bad" in result.failed_segments


async def test_opus_runner_durable_persists_states() -> None:
    opus = Opus(id="o", durable=True)
    opus.add_segments([PipeSegment(id="s1", pipe=_make_pipe())])

    store = InMemoryDurableRunState("o", "run-1")
    result = await OpusRunner().run(opus, state_store=store)

    assert result.success
    state = await store.get_segment_state("s1")
    assert state is not None
    assert state.status == SegmentStatus.SUCCESS


# ---------------------------------------------------------------------------
# RetryPolicy / run_with_retry
# ---------------------------------------------------------------------------


async def test_run_with_retry_succeeds_on_first_attempt() -> None:
    calls = 0

    async def _ok():
        nonlocal calls
        calls += 1
        return 42

    result = await run_with_retry(_ok, RetryPolicy(max_retries=3, backoff_s=0))
    assert result == 42
    assert calls == 1


async def test_run_with_retry_retries_and_succeeds() -> None:
    calls = 0

    async def _flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")
        return "ok"

    result = await run_with_retry(_flaky, RetryPolicy(max_retries=3, backoff_s=0, jitter=False))
    assert result == "ok"
    assert calls == 3


async def test_run_with_retry_raises_after_exhaustion() -> None:
    async def _always_fail():
        raise ValueError("permanent")

    with pytest.raises(ValueError, match="permanent"):
        await run_with_retry(_always_fail, RetryPolicy(max_retries=2, backoff_s=0, jitter=False))


# ---------------------------------------------------------------------------
# DurableRunState (in-memory)
# ---------------------------------------------------------------------------


async def test_in_memory_state_get_and_set() -> None:
    store = InMemoryDurableRunState("opus-1", "run-1")
    state = SegmentState(segment_id="s1", status=SegmentStatus.RUNNING)
    await store.set_segment_state(state)

    loaded = await store.get_segment_state("s1")
    assert loaded is not None
    assert loaded.status == SegmentStatus.RUNNING


async def test_in_memory_state_get_missing_returns_none() -> None:
    store = InMemoryDurableRunState("o", "r")
    assert await store.get_segment_state("ghost") is None


async def test_in_memory_state_get_all() -> None:
    store = InMemoryDurableRunState("o", "r")
    await store.set_segment_state(SegmentState("a", SegmentStatus.SUCCESS))
    await store.set_segment_state(SegmentState("b", SegmentStatus.PENDING))
    all_states = await store.get_all_states()
    assert len(all_states) == 2
    ids = {s.segment_id for s in all_states}
    assert ids == {"a", "b"}


async def test_in_memory_state_heartbeat() -> None:
    import datetime
    store = InMemoryDurableRunState("o", "r")
    assert await store.get_heartbeat() is None
    await store.update_heartbeat()
    hb = await store.get_heartbeat()
    assert hb is not None
    assert hb.tzinfo is not None
