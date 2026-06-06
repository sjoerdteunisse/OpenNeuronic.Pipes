from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record

if TYPE_CHECKING:
    from openneuronic.pipes.lineage.events import LineageEvent
    from openneuronic.pipes.metrics.base import RunMetrics
    from openneuronic.pipes.replay.point import ReplayPoint


@dataclass
class RunResult:
    pipe_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    records_read: int = 0
    records_written: int = 0
    records_dropped: int = 0
    started_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    finished_at: datetime.datetime | None = None
    success: bool = False
    error: Exception | None = None
    metrics: RunMetrics | None = None
    lineage: list[LineageEvent] | None = None
    replay_point: ReplayPoint | None = None


class LocalRunner:
    """In-process runner for local development and testing.

    Reads from the source in batches, applies processors, writes to the sink,
    and advances the bookmark **only** after each successful sink write.  No
    broker, Redis, or containers are required.

    When ``pipe.measures`` is non-empty the runner activates a
    :class:`~openneuronic.pipes.metrics.context.MetricsContext` and attaches a
    :class:`~openneuronic.pipes.metrics.base.RunMetrics` snapshot to the
    returned :class:`RunResult`.  Structured JSON progress is logged via
    :class:`~openneuronic.pipes.observability.logging.PipeLogger`.

    Args:
        batch_size: Maximum number of records buffered before flushing to sink.
    """

    def __init__(self, batch_size: int = 500) -> None:
        self._batch_size = batch_size

    async def run(
        self,
        pipe: Pipe,
        bookmark: Bookmark | None = None,
    ) -> RunResult:
        from openneuronic.pipes.lineage.refs import DatasetRef
        from openneuronic.pipes.lineage.tracker import LineageTracker
        from openneuronic.pipes.metrics.base import MeasureSetRef
        from openneuronic.pipes.metrics.context import MetricsContext
        from openneuronic.pipes.metrics.decorators import set_active_context
        from openneuronic.pipes.observability.logging import PipeLogger
        from openneuronic.pipes.replay.snapshot import create_snapshot

        measures_active = bool(pipe.measures)
        metrics_ctx: MetricsContext | None = None
        if measures_active:
            metrics_ctx = MetricsContext(pipe_id=pipe.id)

        tracker = LineageTracker()
        logger = PipeLogger(pipe_id=pipe.id)

        result = RunResult(pipe_id=pipe.id)

        # Emit dataset-level lineage for source and sink.
        schema_ver = (
            getattr(pipe.schema, "__schema_version__", 1)
            if pipe.schema is not None
            else 1
        )
        contract_ver = (
            getattr(pipe.contract, "__contract_version__", 1)
            if pipe.contract is not None
            else 1
        )
        tracker.track_read(
            pipe.id,
            DatasetRef(
                name=pipe.source.dataset_name,
                schema_name=getattr(pipe.schema, "__name__", ""),
                schema_version=schema_ver,
            ),
            schema_version=schema_ver,
        )

        token = set_active_context(metrics_ctx)
        if metrics_ctx is not None:
            metrics_ctx.start()

        logger.run_start(
            source=type(pipe.source).__name__,
            sink=type(pipe.sink).__name__,
            mode=str(pipe.mode),
        )

        try:
            await pipe.source.setup()
            await pipe.sink.setup()
            for processor in pipe.processors:
                await processor.setup()

            batch: list[Record] = []
            batch_num = 0
            async for record in pipe.source.read(bookmark):
                record.pipe_id = pipe.id
                result.records_read += 1
                batch.append(record)

                if len(batch) >= self._batch_size:
                    batch_num += 1
                    written = await self._flush(pipe, batch, bookmark)
                    result.records_written += written
                    result.records_dropped += len(batch) - written
                    if metrics_ctx is not None:
                        metrics_ctx.record_batch(
                            read=len(batch),
                            written=written,
                            dropped=len(batch) - written,
                        )
                    logger.batch_written(batch_num, len(batch), written)
                    batch = []

            if batch:
                batch_num += 1
                written = await self._flush(pipe, batch, bookmark)
                result.records_written += written
                result.records_dropped += len(batch) - written
                if metrics_ctx is not None:
                    metrics_ctx.record_batch(
                        read=len(batch),
                        written=written,
                        dropped=len(batch) - written,
                    )
                logger.batch_written(batch_num, len(batch), written)

            # Emit write lineage after all batches succeed.
            tracker.track_write(
                pipe.id,
                DatasetRef(
                    name=pipe.sink.dataset_name,
                    schema_name=getattr(pipe.schema, "__name__", ""),
                    schema_version=schema_ver,
                ),
                schema_version=schema_ver,
                contract_version=contract_ver,
            )

            result.success = True

        except Exception as exc:
            result.error = exc
            if metrics_ctx is not None:
                metrics_ctx.record_error()
            raise

        finally:
            result.finished_at = datetime.datetime.now(datetime.UTC)
            for processor in pipe.processors:
                await processor.teardown()
            await pipe.sink.teardown()
            await pipe.source.teardown()

            if metrics_ctx is not None:
                metrics_ctx.stop()
                result.metrics = metrics_ctx.snapshot()

            result.lineage = tracker.events
            if result.success:
                result.replay_point = create_snapshot(
                    pipe,
                    result,
                    bookmark_value=bookmark.value if bookmark else None,
                )

            logger.run_end(result, result.metrics)
            set_active_context(None)

        return result

    async def _flush(
        self,
        pipe: Pipe,
        batch: list[Record],
        bookmark: Bookmark | None,
    ) -> int:
        # Compute the potential new bookmark value from the raw batch before
        # processors may drop or reorder records.
        new_bm_value: Any = None
        if bookmark is not None and pipe.mode == CopyMode.INCREMENTAL:
            new_bm_value = self._compute_new_bookmark_value(bookmark, batch)

        # Apply processors (may drop records by returning None).
        records = batch
        for processor in pipe.processors:
            records = await processor.process_batch(records)

        # Write to sink.  If this raises, the bookmark is NOT advanced.
        await pipe.sink.write(records)

        # Advance bookmark only after the sink write succeeded AND at least
        # one record was written (dropping all records is not considered
        # forward progress).
        if new_bm_value is not None and records and new_bm_value != bookmark.value:  # type: ignore[union-attr]
            updated = Bookmark(
                pipe_id=bookmark.pipe_id,  # type: ignore[union-attr]
                column=bookmark.column,  # type: ignore[union-attr]
                type=bookmark.type,  # type: ignore[union-attr]
                value=new_bm_value,
                previous_value=bookmark.value,  # type: ignore[union-attr]
                batch_count=bookmark.batch_count + 1,  # type: ignore[union-attr]
            )
            await pipe.sink.commit_bookmark(updated)
            # Update the caller's bookmark object in-place after persistence.
            bookmark.previous_value = bookmark.value  # type: ignore[union-attr]
            bookmark.value = new_bm_value  # type: ignore[union-attr]
            bookmark.batch_count += 1  # type: ignore[union-attr]

        return len(records)

    @staticmethod
    def _compute_new_bookmark_value(
        bookmark: Bookmark, records: list[Record]
    ) -> Any:
        col = bookmark.column
        values = [
            r.payload[col]
            for r in records
            if col in r.payload and r.payload[col] is not None
        ]
        if not values:
            return None
        try:
            return max(values)
        except TypeError:
            return None
