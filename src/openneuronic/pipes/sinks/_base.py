from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.record import Record

if TYPE_CHECKING:
    from openneuronic.pipes.core.partial_scope import PartialScope


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
        """Persist the bookmark state after a successful write."""

    @abstractmethod
    async def teardown(self) -> None:
        """Release connections and resources."""

    async def prepare_full_load(self) -> None:
        """Called once before the write loop for ``CopyMode.FULL``.

        Should create and truncate a staging table so that :meth:`write`
        directs records there instead of the live table.
        Default implementation is a no-op (direct overwrite).
        """

    async def commit_full_load(self) -> None:
        """Called once after all batches have been written for ``CopyMode.FULL``.

        Should atomically swap the staging table with the live table.
        Default implementation is a no-op.
        """

    async def delete_scope(self, scope: PartialScope) -> None:
        """Delete rows matching *scope* from the live table before a partial
        reload writes replacement data.

        Default implementation is a no-op (subclasses override).
        """

    async def apply_schema(self, schema_cls: type) -> None:
        """Create the target table from the abstract schema DDL if it does
        not yet exist, and record the migration in the migration ledger.

        Calls :meth:`ensure_migration_ledger` then executes the DDL generated
        by the appropriate mapper.  Subclasses that support auto-migration
        must override this.
        """

    @property
    def dataset_name(self) -> str:
        return getattr(self, "_table", None) or type(self).__name__
