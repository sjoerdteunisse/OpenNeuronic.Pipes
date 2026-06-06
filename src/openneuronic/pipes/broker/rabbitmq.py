"""RabbitMQ broker implementation using aio-pika.

Requires the ``broker`` optional extra::

    pip install 'openneuronic-pipes[broker]'
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from openneuronic.pipes.broker._base import (
    AbstractBroker,
    _bytes_to_record,
    _record_to_bytes,
)
from openneuronic.pipes.core.record import Record


class RabbitMQBroker(AbstractBroker):
    """AMQP broker backed by `aio-pika <https://aio-pika.readthedocs.io/>`_.

    Each *topic* maps to an AMQP queue named ``onpipes.{topic}``.  A
    dead-letter exchange ``onpipes.dlx`` is declared so that rejected
    messages route to ``onpipes.{topic}.dlq`` automatically.

    Args:
        url: AMQP URL, e.g. ``"amqp://guest:guest@localhost/"``
        prefetch: Consumer prefetch count (QoS).
    """

    DLX = "onpipes.dlx"

    def __init__(self, url: str, prefetch: int = 10) -> None:
        try:
            import aio_pika  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aio-pika is required for RabbitMQBroker. "
                "Install with: pip install 'openneuronic-pipes[broker]'"
            ) from exc
        self._url = url
        self._prefetch = prefetch
        self._connection: Any = None
        self._channel: Any = None

    async def setup(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch)
        # Declare dead-letter exchange
        await self._channel.declare_exchange(
            self.DLX, aio_pika.ExchangeType.DIRECT, durable=True
        )

    async def teardown(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None
            self._channel = None

    def _queue_name(self, topic: str) -> str:
        return f"onpipes.{topic}"

    async def _ensure_queue(self, topic: str) -> Any:
        import aio_pika

        dlx = await self._channel.get_exchange(self.DLX)
        dlq_name = f"onpipes.{topic}.dlq"
        # DLQ queue (plain, no further routing)
        dlq = await self._channel.declare_queue(dlq_name, durable=True)
        await dlq.bind(dlx, routing_key=dlq_name)
        # Main queue with dead-letter config
        return await self._channel.declare_queue(
            self._queue_name(topic),
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.DLX,
                "x-dead-letter-routing-key": dlq_name,
            },
        )

    async def publish(self, topic: str, records: list[Record]) -> None:
        import aio_pika

        if self._channel is None:
            raise RuntimeError("RabbitMQBroker.setup() must be called first")
        await self._ensure_queue(topic)
        default_exchange = self._channel.default_exchange
        for record in records:
            await default_exchange.publish(
                aio_pika.Message(body=_record_to_bytes(record), delivery_mode=2),
                routing_key=self._queue_name(topic),
            )

    def consume(self, topic: str, *, batch_size: int = 100) -> AsyncIterator[Record]:
        async def _gen() -> AsyncIterator[Record]:
            if self._channel is None:
                raise RuntimeError("RabbitMQBroker.setup() must be called first")
            queue = await self._ensure_queue(topic)
            count = 0
            async with queue.iterator() as it:
                async for message in it:
                    async with message.process():
                        yield _bytes_to_record(message.body)
                        count += 1
                        if count >= batch_size:
                            break

        return _gen()

    async def publish_dlq(
        self, topic: str, records: list[Record], reason: str
    ) -> None:
        import aio_pika

        if self._channel is None:
            raise RuntimeError("RabbitMQBroker.setup() must be called first")
        dlx = await self._channel.get_exchange(self.DLX)
        dlq_name = f"onpipes.{topic}.dlq"
        for record in records:
            body = _record_to_bytes(record)
            headers = {"x-dlq-reason": reason}
            await dlx.publish(
                aio_pika.Message(body=body, headers=headers, delivery_mode=2),
                routing_key=dlq_name,
            )
