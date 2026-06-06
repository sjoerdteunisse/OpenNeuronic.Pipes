from __future__ import annotations

from abc import ABC, abstractmethod

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.record import Record


class AbstractSink(ABC):
    @abstractmethod
    async def setup(self) -> None:
        """Acquire connections and resources."""

    @abstractmethod
    async def write(self, records: list[Record]) -> None:
        """Persist a batch of records.

        Must raise on failure so the caller can withhold the bookmark advance.
        """

    @abstractmethod
    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        """Persist the bookmark state after a successful write.

        Receives the fully updated ``Bookmark`` (value already advanced by the
        runner).  Implementations may write to Redis, a metadata store, or a
        local file depending on the deployment tier.
        """

    @abstractmethod
    async def teardown(self) -> None:
        """Release connections and resources."""

    @property
    def dataset_name(self) -> str:
        """Human-readable dataset identifier used for lineage tracking.

        Subclasses should override this to return a meaningful name such as
        ``"sqlserver.test.Orders"``.  The default falls back to ``_table``
        if present, otherwise the class name.
        """
        return getattr(self, "_table", None) or type(self).__name__
