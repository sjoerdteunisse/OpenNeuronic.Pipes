from __future__ import annotations

from abc import ABC, abstractmethod

from openneuronic.pipes.core.record import Record


class Processor(ABC):
    async def setup(self) -> None:
        """Called once before processing begins. Override for resource acquisition."""

    @abstractmethod
    async def process(self, record: Record) -> Record | None:
        """Transform a single record.

        Return the (possibly modified) record to keep it, or ``None`` to drop it.
        """

    async def process_batch(self, records: list[Record]) -> list[Record]:
        """Process a batch of records, dropping any for which ``process`` returns ``None``."""
        out: list[Record] = []
        for record in records:
            result = await self.process(record)
            if result is not None:
                out.append(result)
        return out

    async def teardown(self) -> None:
        """Called once after processing completes. Override for resource cleanup."""
