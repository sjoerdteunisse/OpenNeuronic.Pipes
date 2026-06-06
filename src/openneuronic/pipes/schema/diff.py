from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from openneuronic.pipes.schema.field import Field

if TYPE_CHECKING:
    from openneuronic.pipes.schema.base import Schema


class ChangeKind(StrEnum):
    SAFE     = "safe"
    BREAKING = "breaking"
    REMOVED  = "removed"


@dataclass(frozen=True)
class FieldChange:
    name: str
    kind: ChangeKind
    description: str
    old: Field | None = None
    new: Field | None = None


@dataclass
class SchemaDiff:
    from_version: int
    to_version: int
    changes: list[FieldChange]

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.kind == ChangeKind.BREAKING for c in self.changes)

    @property
    def breaking(self) -> list[FieldChange]:
        return [c for c in self.changes if c.kind == ChangeKind.BREAKING]

    @property
    def safe(self) -> list[FieldChange]:
        return [c for c in self.changes if c.kind == ChangeKind.SAFE]

    @property
    def removed(self) -> list[FieldChange]:
        return [c for c in self.changes if c.kind == ChangeKind.REMOVED]


def diff_schemas(
    old: type[Schema],
    new: type[Schema],
) -> SchemaDiff:
    """Compare two schema versions and classify each change."""
    old_fields = old.__schema_fields__
    new_fields = new.__schema_fields__
    changes: list[FieldChange] = []

    for name, old_f in old_fields.items():
        if name not in new_fields:
            changes.append(FieldChange(
                name=name,
                kind=ChangeKind.BREAKING,
                description=f"Field {name!r} was dropped",
                old=old_f,
            ))
        else:
            new_f = new_fields[name]
            _compare_field(name, old_f, new_f, changes)

    for name, new_f in new_fields.items():
        if name not in old_fields:
            if new_f.nullable or new_f.has_default:
                kind = ChangeKind.SAFE
                desc = f"Field {name!r} added (nullable={new_f.nullable})"
            else:
                kind = ChangeKind.BREAKING
                desc = (
                    f"Field {name!r} added as non-nullable with no default — "
                    "existing rows would be invalid"
                )
            changes.append(FieldChange(name=name, kind=kind, description=desc, new=new_f))

    return SchemaDiff(
        from_version=old.__schema_version__,
        to_version=new.__schema_version__,
        changes=changes,
    )


def _compare_field(
    name: str,
    old_f: Field,
    new_f: Field,
    changes: list[FieldChange],
) -> None:
    # Type change
    if old_f.type != new_f.type:
        changes.append(FieldChange(
            name=name,
            kind=ChangeKind.BREAKING,
            description=(
                f"Field {name!r} type changed from {old_f.type!r} to {new_f.type!r} "
                "without explicit coercion"
            ),
            old=old_f, new=new_f,
        ))

    # Nullability narrowed (nullable → non-nullable) is breaking.
    elif old_f.nullable and not new_f.nullable:
        changes.append(FieldChange(
            name=name,
            kind=ChangeKind.BREAKING,
            description=(
                f"Field {name!r} changed from nullable to non-nullable "
                "without a backfill"
            ),
            old=old_f, new=new_f,
        ))

    # max_length narrowed
    elif (
        old_f.max_length is not None
        and new_f.max_length is not None
        and new_f.max_length < old_f.max_length
    ):
        changes.append(FieldChange(
            name=name,
            kind=ChangeKind.BREAKING,
            description=(
                f"Field {name!r} max_length narrowed from "
                f"{old_f.max_length} to {new_f.max_length}"
            ),
            old=old_f, new=new_f,
        ))

    # max_length widened
    elif (
        old_f.max_length is not None
        and new_f.max_length is not None
        and new_f.max_length > old_f.max_length
    ):
        changes.append(FieldChange(
            name=name,
            kind=ChangeKind.SAFE,
            description=f"Field {name!r} max_length widened to {new_f.max_length}",
            old=old_f, new=new_f,
        ))

    # Nullable widened (non-nullable → nullable) — safe
    elif not old_f.nullable and new_f.nullable:
        changes.append(FieldChange(
            name=name,
            kind=ChangeKind.SAFE,
            description=f"Field {name!r} relaxed to nullable",
            old=old_f, new=new_f,
        ))
