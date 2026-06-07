"""BrokerPipeRunner — broker-mediated source → processor worker → sink execution.

This runner implements the spec's distributed runtime model:

    Source reader   →  broker queue (raw topic)
    Processor worker ←  broker queue (raw topic)
                     →  broker queue (processed topic)
    Sink writer      ←  broker queue (processed topic)

All three roles run in-process for the local development tier but can be
separated into independent containers by pointing each role at a shared
broker (e.g. RabbitMQ or Kafka).

In production, deploy each role separately:
- Source reader publishes to ``{pipe_id}.raw``
- Processor worker consumes from ``{pipe_id}.raw``, publishes to ``{pipe_id}.processed``
- Sink writer consumes from ``{pipe_id}.processed`` and commits to the database

Dead-letter records are routed to ``{pipe_id}.raw.dlq`` / ``{pipe_id}.processed.dlq``
via :meth:`~openneuronic.pipes.broker._base.AbstractBroker.publish_dlq`.
"""
from __future__ import annotations

import asyncio
import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any

from openneuronic.pipes.broker._base import AbstractBroker
from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.runner import RunResult


@dataclass
class BrokerRunResult:
    """Result of a broker-mediated pipe run."""

    pipe_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    records_published: int = 0
    records_processed: int = 0
    records_written: int = 0
    records_dlq: int = 0
    started_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    finished_at: datetime.datetime | None = None
    success: bool = False
    error: Exception | None = None


class BrokerPipeRunner:
    """Runs the three-stage broker pipeline in-process (for local/test use).

    For production, call each stage method independently from separate workers:

    - :meth:`read_to_broker` — source reader role
    - :meth:`process_from_broker` — processor worker role
    - :meth:`write_from_broker` — sink writer role

    Args:
        broker: Any :class:`~openneuronic.pipes.broker._base.AbstractBroker`
            implementation.
        batch_size: Number of records buffered per broker publish / sink write.
    """

    def __init__(
        self,
        broker: AbstractBroker,
        batch_size: int = 500,
    ) -> None:
        self._broker = broker
        self._batch_size = batch_size

    # ------------------------------------------------------------------
    # Convenience: run all three stages sequentially in-process
    # ------------------------------------------------------------------

    async def run(
        self,
        pipe: Pipe,
        run_id: str | None = None,
    ) -> BrokerRunResult:
        """Execute the full pipeline in-process through the broker."""
        run_id = run_id or str(uuid.uuid4())
        result = BrokerRunResult(pipe_id=pipe.id, run_id=run_id)

        raw_topic = f"{pipe.id}.raw"
        processed_topic = f"{pipe.id}.processed"

        await self._broker.setup()
        try:
            # Stage 1: read source → publish to raw topic
            result.records_published = await self.read_to_broker(pipe, raw_topic)

            # Stage 2: consume raw → process → publish to processed topic
            result.records_processed = await self.process_from_broker(
                pipe, raw_topic, processed_topic
            )

            # Stage 3: consume processed → write to sink
            result.records_written = await self.write_from_broker(
                pipe, processed_topic
            )

            result.success = True

        except Exception as exc:
            result.error = exc
            raise

        finally:
            result.finished_at = datetime.datetime.now(datetime.UTC)
            await self._broker.teardown()

        return result

    # ------------------------------------------------------------------
    # Stage 1 — Source reader
    # ------------------------------------------------------------------

    async def read_to_broker(
        self,
        pipe: Pipe,
        topic: str,
        bookmark: Any = None,
    ) -> int:
        """Read all records from the source and publish them to *topic*.

        Returns the number of records published.
        """
        published = 0
        await pipe.source.setup()
        try:
            batch: list[Record] = []
            async for record in pipe.source.read(bookmark):
                record.pipe_id = pipe.id
                batch.append(record)
                if len(batch) >= self._batch_size:
                    await self._broker.publish(topic, batch)
                    published += len(batch)
                    batch = []
            if batch:
                await self._broker.publish(topic, batch)
                published += len(batch)
        finally:
            await pipe.source.teardown()
        return published

    # ------------------------------------------------------------------
    # Stage 2 — Processor worker
    # ------------------------------------------------------------------

    async def process_from_broker(
        self,
        pipe: Pipe,
        in_topic: str,
        out_topic: str,
    ) -> int:
        """Consume from *in_topic*, apply processors, publish to *out_topic*.

        Returns the number of records published to *out_topic*.
        """
        if not pipe.processors:
            # No processors — forward directly without consuming.
            processed = 0
            async for record in self._broker.consume(in_topic, batch_size=self._batch_size):
                processed += 1
                await self._broker.publish(out_topic, [record])
            return processed

        for processor in pipe.processors:
            await processor.setup()
        processed = 0
        try:
            async for record in self._broker.consume(in_topic, batch_size=self._batch_size):
                try:
                    transformed: list[Record] = await pipe.processors[0].process_batch([record])
                    for p in pipe.processors[1:]:
                        transformed = await p.process_batch(transformed)
                    if transformed:
                        await self._broker.publish(out_topic, transformed)
                        processed += len(transformed)
                    # dropped records → no DLQ needed (processor intentionally dropped)
                except Exception as exc:
                    await self._broker.publish_dlq(in_topic, [record], reason=str(exc))
        finally:
            for processor in pipe.processors:
                await processor.teardown()
        return processed

    # ------------------------------------------------------------------
    # Stage 3 — Sink writer
    # ------------------------------------------------------------------

    async def write_from_broker(
        self,
        pipe: Pipe,
        topic: str,
    ) -> int:
        """Consume from *topic* and write to the sink.

        Returns the number of records written.
        """
        written = 0
        await pipe.sink.setup()
        try:
            if getattr(pipe.sink, "_auto_migrate", False) and pipe.schema is not None:
                await pipe.sink.apply_schema(pipe.schema)
            if pipe.mode == CopyMode.FULL:
                await pipe.sink.prepare_full_load()
            if pipe.mode == CopyMode.PARTIAL and pipe.scope is not None:
                await pipe.sink.delete_scope(pipe.scope)

            batch: list[Record] = []
            async for record in self._broker.consume(topic, batch_size=self._batch_size):
                batch.append(record)
                if len(batch) >= self._batch_size:
                    try:
                        await pipe.sink.write(batch)
                        written += len(batch)
                    except Exception as exc:
                        await self._broker.publish_dlq(topic, batch, reason=str(exc))
                    batch = []
            if batch:
                try:
                    await pipe.sink.write(batch)
                    written += len(batch)
                except Exception as exc:
                    await self._broker.publish_dlq(topic, batch, reason=str(exc))

            if pipe.mode == CopyMode.FULL:
                await pipe.sink.commit_full_load()
        finally:
            await pipe.sink.teardown()
        return written
