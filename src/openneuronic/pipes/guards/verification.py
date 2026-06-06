from __future__ import annotations

from typing import TYPE_CHECKING

from openneuronic.pipes.guards.base import Guard, GuardContext, GuardResult

if TYPE_CHECKING:
    pass


class RowCountGuard(Guard):
    """Fails when the batch size falls outside the expected range.

    Args:
        min_rows: Minimum acceptable row count (inclusive). ``None`` = no lower bound.
        max_rows: Maximum acceptable row count (inclusive). ``None`` = no upper bound.
    """

    def __init__(
        self,
        min_rows: int | None = None,
        max_rows: int | None = None,
    ) -> None:
        self._min = min_rows
        self._max = max_rows

    async def check(self, ctx: GuardContext) -> GuardResult:
        count = len(ctx.batch)
        if self._min is not None and count < self._min:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=f"Row count {count} is below minimum {self._min}",
            )
        if self._max is not None and count > self._max:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=f"Row count {count} exceeds maximum {self._max}",
            )
        return GuardResult(guard_name=self.name, passed=True)


class DuplicateCheckGuard(Guard):
    """Fails when the batch contains duplicate values in the given key fields.

    Args:
        key_fields: One or more payload field names that together form a unique key.
    """

    def __init__(self, key_fields: list[str]) -> None:
        self._keys = key_fields

    async def check(self, ctx: GuardContext) -> GuardResult:
        seen: set[tuple] = set()
        duplicates: list[tuple] = []
        for record in ctx.batch:
            key = tuple(record.payload.get(k) for k in self._keys)
            if key in seen:
                duplicates.append(key)
            else:
                seen.add(key)
        if duplicates:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Found {len(duplicates)} duplicate key(s) on fields "
                    f"{self._keys!r}: {duplicates[:3]}{'...' if len(duplicates) > 3 else ''}"
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)


class SchemaMatchGuard(Guard):
    """Fails when any record contains fields not declared in the schema.

    Args:
        schema_cls: A :class:`~openneuronic.pipes.schema.base.Schema` subclass.
    """

    def __init__(self, schema_cls: type) -> None:
        self._schema = schema_cls
        self._known: frozenset[str] = frozenset(schema_cls.__schema_fields__.keys())

    async def check(self, ctx: GuardContext) -> GuardResult:
        unknown_per_record: list[str] = []
        for i, record in enumerate(ctx.batch):
            extra = set(record.payload.keys()) - self._known
            if extra:
                unknown_per_record.append(f"record[{i}]: {sorted(extra)}")
        if unknown_per_record:
            return GuardResult(
                guard_name=self.name,
                passed=False,
                message=(
                    f"Unknown fields found (not in schema {self._schema.__name__}): "
                    + "; ".join(unknown_per_record[:3])
                ),
            )
        return GuardResult(guard_name=self.name, passed=True)
