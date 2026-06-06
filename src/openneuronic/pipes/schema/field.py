from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from openneuronic.pipes.schema.field_type import FieldType

_SENTINEL = object()


@dataclass
class Field:
    """Abstract field definition inside a :class:`Schema`.

    Parameters
    ----------
    type:
        Abstract field type.  Never a database-native type.
    nullable:
        Whether the field may contain ``None`` / ``NULL``.
    primary_key:
        Whether this field is (part of) the primary key.
    max_length:
        Maximum character length for ``STRING`` / ``CHAR`` fields.
    precision:
        Total digits for ``DECIMAL`` fields.
    scale:
        Decimal digits for ``DECIMAL`` fields.
    default:
        Python-side default value used by the runtime upgrader when filling
        a new non-nullable field during a migration.  Use the sentinel
        ``REQUIRED`` (default) to indicate no default exists.
    array_item_type:
        For ``ARRAY`` fields, the element type.
    """

    type: FieldType
    nullable: bool = True
    primary_key: bool = False
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    default: Any = field(default=_SENTINEL)
    array_item_type: FieldType | None = None

    @property
    def has_default(self) -> bool:
        return self.default is not _SENTINEL
