from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record


class AbstractSource(ABC):
    @abstractmethod
    async def setup(self) -> None:
        """Acquire connections and resources."""

    @abstractmethod
    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        """Yield records from the source.

        Implementations should be async generators (``async def`` with ``yield``).
        If a bookmark is supplied the source must constrain the result set to
        records that come *after* the bookmark value.
        """

    @abstractmethod
    async def teardown(self) -> None:
        """Release connections and resources."""

    @property
    def dataset_name(self) -> str:
        """Human-readable dataset identifier used for lineage tracking.

        Subclasses should override this to return a meaningful name such as
        ``"sqlserver.now.Orders"``.  The default falls back to the class name.
        """
        return getattr(self, "source_id", None) or type(self).__name__
