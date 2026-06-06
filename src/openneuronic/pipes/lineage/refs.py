"""Lineage reference types — lightweight value objects used everywhere."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetRef:
    """Identifies a dataset (table, view, or file) by name.

    Attributes:
        name: Fully qualified or logical name, e.g. ``"sqlserver.now.Orders"``.
        schema_name: Name of the abstract :class:`~openneuronic.pipes.schema.base.Schema`
            class, or empty string when unknown.
        schema_version: Schema version number at the time of the event.
    """

    name: str
    schema_name: str = ""
    schema_version: int = 1


@dataclass(frozen=True)
class ColumnRef:
    """Identifies a single column within a dataset.

    Attributes:
        dataset: The parent :class:`DatasetRef`.
        column_name: Column / field name in the abstract schema vocabulary.
    """

    dataset: DatasetRef
    column_name: str
