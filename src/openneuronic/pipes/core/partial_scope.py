from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PartialScope:
    """Defines the scope for a :attr:`~openneuronic.pipes.core.enums.CopyMode.PARTIAL` copy.

    Only the fields that are set are applied.  Multiple constraints are
    combined with ``AND`` semantics when :meth:`to_where_clause` is called.

    Attributes:
        date_start: Inclusive lower bound for a datetime-column window.
        date_end:   Inclusive upper bound for a datetime-column window.
        date_column: Column name used for the date window filter.
        key_set: Explicit set of primary key values to include.
        key_column: Column name used for key-set filtering.
        predicate: Raw SQL predicate appended verbatim — use only with trusted
            values to avoid SQL injection.
        partition_id: Optional partition identifier for partitioned sources.
    """

    date_start: datetime.datetime | None = None
    date_end: datetime.datetime | None = None
    date_column: str | None = None
    key_set: list[Any] = field(default_factory=list)
    key_column: str | None = None
    predicate: str | None = None
    partition_id: str | None = None

    def to_where_clause(self) -> str:
        """Build a SQL ``WHERE`` clause fragment from the scope fields.

        Returns an empty string when no constraints are set.
        """
        parts: list[str] = []

        if self.date_column and (self.date_start or self.date_end):
            if self.date_start:
                parts.append(
                    f"{self.date_column} >= '{self.date_start.isoformat()}'"
                )
            if self.date_end:
                parts.append(
                    f"{self.date_column} <= '{self.date_end.isoformat()}'"
                )

        if self.key_column and self.key_set:
            ids = ", ".join(repr(k) for k in self.key_set)
            parts.append(f"{self.key_column} IN ({ids})")

        if self.predicate:
            parts.append(f"({self.predicate})")

        return " AND ".join(parts)
