"""Kafka broker implementation using aiokafka.

Requires the ``broker`` optional extra::

    pip install 'openneuronic-pipes[broker]'
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from openneuronic.pipes.broker._base import (
    AbstractBroker,
    _bytes_to_record,
    _record_to_bytes,
)
from openneuronic.pipes.core.record import Record


class KafkaBroker(AbstractBroker):
    """Apache Kafka broker backed by `aiokafka <https://aiokafka.readthedocs.io/>`_.

    Each *topic* maps to a Kafka topic.  The DLQ is a separate topic named
    ``{topic}.dlq``.

    Args:
        bootstrap_servers: Comma-separated ``host:port`` pairs.
        group_id: Consumer group ID for :meth:`consume`.
        auto_offset_reset: Where to start consuming when no committed offset
            exists (``"earliest"`` or ``"latest"``).
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "onpipes",
        auto_offset_reset: str = "earliest",
    ) -> None:
        try:
            import aiokafka  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aiokafka is required for KafkaBroker. "
                "Install with: pip install 'openneuronic-pipes[broker]'"
            ) from exc
        self._servers = bootstrap_servers
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._producer: Any = None

    async def setup(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)
        await self._producer.start()

    async def teardown(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def publish(self, topic: str, records: list[Record]) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBroker.setup() must be called first")
        for record in records:
            await self._producer.send_and_wait(topic, _record_to_bytes(record))

    def consume(self, topic: str, *, batch_size: int = 100) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            from aiokafka import AIOKafkaConsumer

            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self._servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=False,
            )
            await consumer.start()
            count = 0
            try:
                async for msg in consumer:
                    yield _bytes_to_record(msg.value)
                    await consumer.commit()
                    count += 1
                    if count >= batch_size:
                        break
            finally:
                await consumer.stop()

        return _gen()

    async def publish_dlq(
        self, topic: str, records: list[Record], reason: str
    ) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBroker.setup() must be called first")
        dlq_topic = f"{topic}.dlq"
        for record in records:
            await self._producer.send_and_wait(dlq_topic, _record_to_bytes(record))
