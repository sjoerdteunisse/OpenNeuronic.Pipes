"""OpusRunner — executes an Opus with parallel branches and durable state."""
from __future__ import annotations

import asyncio
import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any

from openneuronic.pipes.core.enums import GraphFailureMode, SegmentStatus
from openneuronic.pipes.opus.durability import filter_pending_segments
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.retry import RetryPolicy, run_with_retry
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.opus.state import DurableRunState, InMemoryDurableRunState, SegmentState
from openneuronic.pipes.runner import LocalRunner, RunResult


@dataclass
class SegmentRunResult:
    """Result of executing one :class:`~openneuronic.pipes.opus.segment.PipeSegment`."""

    segment_id: str
    run_result: RunResult | None = None
    success: bool = False
    retries: int = 0
    error: str | None = None


@dataclass
class OpusRunResult:
    """Aggregate result of a full :class:`~openneuronic.pipes.opus.opus.Opus` run."""

    opus_id: str
    run_id: str
    segment_results: dict[str, SegmentRunResult] = field(default_factory=dict)
    success: bool = False
    started_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    finished_at: datetime.datetime | None = None

    @property
    def failed_segments(self) -> list[str]:
        return [sid for sid, r in self.segment_results.items() if not r.success]


class OpusRunner:
    """Executes an :class:`~openneuronic.pipes.opus.opus.Opus`.

    Runs independent segments concurrently using :func:`asyncio.gather` and
    respects ``depends_on`` ordering via topological waves.  When
    ``durable=True`` on the :class:`Opus`, segment lifecycle transitions are
    persisted to a :class:`~openneuronic.pipes.opus.state.DurableRunState`
    so the run can be resumed after a crash.

    Args:
        batch_size: Forwarded to the internal :class:`~openneuronic.pipes.runner.LocalRunner`.
        default_retry_policy: Fallback retry policy for segments that do not
            specify their own.  Defaults to a single retry with 1 s back-off.
    """

    def __init__(
        self,
        batch_size: int = 500,
        default_retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._local_runner = LocalRunner(batch_size=batch_size)
        self._default_retry = default_retry_policy or RetryPolicy(max_retries=1, backoff_s=1.0)

    async def run(
        self,
        opus: Opus,
        state_store: DurableRunState | None = None,
        run_id: str | None = None,
    ) -> OpusRunResult:
        """Execute *opus*.

        Args:
            opus: The opus to run.
            state_store: Optional durable state store.  When ``None`` and
                ``opus.durable`` is ``True``, an
                :class:`~openneuronic.pipes.opus.state.InMemoryDurableRunState`
                is created automatically.
            run_id: Optional run identifier.  Generated as a UUID when omitted.

        Returns:
            An :class:`OpusRunResult` summarising all segment outcomes.
        """
        run_id = run_id or str(uuid.uuid4())
        store = state_store or (
            InMemoryDurableRunState(opus.id, run_id) if opus.durable else None
        )

        opus_result = OpusRunResult(opus_id=opus.id, run_id=run_id)

        # Determine which segments still need to run (resume semantics).
        if store is not None:
            pending_ids = {s.id for s in await filter_pending_segments(opus, store)}
        else:
            pending_ids = {s.id for s in opus.segments}

        waves = opus.topological_waves()

        try:
            for wave in waves:
                # Filter to only segments that are still pending in this wave.
                to_run = [s for s in wave if s.id in pending_ids]
                if not to_run:
                    continue

                # Run all segments in this wave concurrently.
                tasks = [
                    self._run_segment(seg, store, opus_result)
                    for seg in to_run
                ]
                await asyncio.gather(*tasks, return_exceptions=False)

                # Check failure policy after each wave.
                failed = [
                    sid
                    for sid in (s.id for s in to_run)
                    if not opus_result.segment_results.get(sid, SegmentRunResult(sid)).success
                ]
                if failed:
                    if opus.on_failure == GraphFailureMode.FAIL_FAST:
                        raise RuntimeError(
                            f"Opus {opus.id!r} aborted (FAIL_FAST): "
                            f"segments failed — {failed}"
                        )
                    elif opus.on_failure == GraphFailureMode.COMPENSATE_AND_STOP:
                        raise RuntimeError(
                            f"Opus {opus.id!r} stopped for compensation: "
                            f"segments failed — {failed}"
                        )
                    # CONTINUE_INDEPENDENT_BRANCHES, RETRY_BRANCH, RETRY_OPUS
                    # → allow subsequent independent waves to proceed.

            opus_result.success = all(
                r.success for r in opus_result.segment_results.values()
            )

        finally:
            opus_result.finished_at = datetime.datetime.now(datetime.UTC)
            if store is not None:
                await store.update_heartbeat()

        return opus_result

    async def _run_segment(
        self,
        segment: PipeSegment,
        store: DurableRunState | None,
        opus_result: OpusRunResult,
    ) -> None:
        policy = segment.retry_policy or self._default_retry

        seg_result = SegmentRunResult(segment_id=segment.id)
        opus_result.segment_results[segment.id] = seg_result

        if store is not None:
            await store.set_segment_state(
                SegmentState(
                    segment_id=segment.id,
                    status=SegmentStatus.RUNNING,
                    started_at=datetime.datetime.now(datetime.UTC),
                )
            )

        async def _attempt() -> RunResult:
            return await self._local_runner.run(segment.pipe)

        try:
            run_result = await run_with_retry(_attempt, policy)
            seg_result.run_result = run_result
            seg_result.success = run_result.success

            if store is not None:
                await store.set_segment_state(
                    SegmentState(
                        segment_id=segment.id,
                        status=SegmentStatus.SUCCESS,
                        finished_at=datetime.datetime.now(datetime.UTC),
                    )
                )

        except Exception as exc:
            seg_result.error = str(exc)
            seg_result.retries = policy.max_retries

            if store is not None:
                await store.set_segment_state(
                    SegmentState(
                        segment_id=segment.id,
                        status=SegmentStatus.FAILED,
                        finished_at=datetime.datetime.now(datetime.UTC),
                        error=str(exc),
                    )
                )
            # Do not re-raise — caller checks opus_result for failures.
