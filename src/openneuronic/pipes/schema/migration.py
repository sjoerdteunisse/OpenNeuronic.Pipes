from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.core.record import Record


class MigrationStepKind(StrEnum):
    RENAME   = "rename"
    FILL     = "fill"
    DROP     = "drop"
    ADD      = "add"
    CAST     = "cast"


@dataclass(frozen=True)
class MigrationStep:
    kind: MigrationStepKind
    field: str | None = None
    old_name: str | None = None
    new_name: str | None = None
    value: Any = None
    cast_fn: Any = None  # callable(value) -> value, not frozen-friendly but practical

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def rename(cls, old: str, new: str) -> MigrationStep:
        return cls(kind=MigrationStepKind.RENAME, old_name=old, new_name=new)

    @classmethod
    def fill(cls, field_name: str, value: Any) -> MigrationStep:
        return cls(kind=MigrationStepKind.FILL, field=field_name, value=value)

    @classmethod
    def drop(cls, field_name: str) -> MigrationStep:
        return cls(kind=MigrationStepKind.DROP, field=field_name)

    @classmethod
    def add(cls, field_name: str, value: Any = None) -> MigrationStep:
        return cls(kind=MigrationStepKind.ADD, field=field_name, value=value)

    @classmethod
    def cast(cls, field_name: str, cast_fn: Any) -> MigrationStep:
        return cls(kind=MigrationStepKind.CAST, field=field_name, cast_fn=cast_fn)

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Apply this step to a payload dict and return the modified dict."""
        p = dict(payload)
        match self.kind:
            case MigrationStepKind.RENAME:
                if self.old_name in p:
                    p[self.new_name] = p.pop(self.old_name)
            case MigrationStepKind.FILL:
                if self.field not in p or p[self.field] is None:
                    p[self.field] = self.value
            case MigrationStepKind.DROP:
                p.pop(self.field, None)
            case MigrationStepKind.ADD:
                if self.field not in p:
                    p[self.field] = self.value
            case MigrationStepKind.CAST:
                if self.field in p and p[self.field] is not None:
                    p[self.field] = self.cast_fn(p[self.field])
        return p


@dataclass
class Migration:
    """Describes how to move records from one schema version to the next.

    Both the in-flight payload transformation (``steps``) and the DDL
    required at the sink (``ddl_up``) are captured here so the same
    artefact drives both runtime and deployment behaviour.
    """

    from_version: int
    to_version: int
    steps: list[MigrationStep] = field(default_factory=list)
    ddl_up: dict[str, str] = field(default_factory=dict)

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Apply all steps in order and return the transformed payload."""
        for step in self.steps:
            payload = step.apply(payload)
        return payload


class MigrationRegistry:
    """Registry of :class:`Migration` objects keyed by
    ``(schema_name, from_version, to_version)``."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, int, int], Migration] = {}

    def register(self, schema_name: str, migration: Migration) -> None:
        key = (schema_name, migration.from_version, migration.to_version)
        self._store[key] = migration

    def get(
        self, schema_name: str, from_version: int, to_version: int
    ) -> Migration | None:
        return self._store.get((schema_name, from_version, to_version))


migration_registry = MigrationRegistry()


class RuntimeUpgrader:
    """Upgrades a record payload from an older schema version to the latest.

    The upgrader walks the chain of :class:`Migration` objects registered
    in ``migration_registry``, applying each step in order.  Unknown versions
    are left unchanged so old producers keep working while sinks move forward.
    """

    def upgrade(
        self,
        payload: dict[str, Any],
        schema_name: str,
        from_version: int,
        to_version: int,
    ) -> dict[str, Any]:
        if from_version >= to_version:
            return payload

        from openneuronic.pipes.schema.registry import schema_registry  # avoid circular
        path = schema_registry.migration_path(schema_name, from_version, to_version)

        current_payload = dict(payload)
        current_version = from_version

        for next_version in path:
            migration = migration_registry.get(schema_name, current_version, next_version)
            if migration is None:
                raise ValueError(
                    f"No migration registered for {schema_name!r} "
                    f"{current_version} → {next_version}"
                )
            current_payload = migration.apply(current_payload)
            current_version = next_version

        return current_payload


runtime_upgrader = RuntimeUpgrader()
