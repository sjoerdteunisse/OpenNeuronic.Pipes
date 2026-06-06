from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.schema.base import Schema


def _family_name(schema_cls: type) -> str:
    """Walk the ``__schema_previous__`` chain to find the root class name.

    All versions in the same lineage share this root name, which is used as
    the family key in the registry.
    """
    current = schema_cls
    while True:
        previous = getattr(current, "__schema_previous__", None)
        if previous is None:
            return current.__name__
        current = previous


class SchemaRegistry:
    """Global registry mapping (family_name, version) → Schema class.

    The *family_name* is the ``__name__`` of the root schema in the ``previous``
    chain.  All versions of the same schema share this key so that
    :meth:`migration_path` can find every intermediate version.

    Use :func:`~openneuronic.pipes.schema.base.schema_version` to register
    schemas automatically.  The registry is also used by the runtime upgrader
    to resolve migration chains.
    """

    def __init__(self) -> None:
        self._store: dict[tuple[str, int], type[Schema]] = {}

    def register(self, schema_cls: type[Schema]) -> None:
        family = _family_name(schema_cls)
        key = (family, schema_cls.__schema_version__)
        self._store[key] = schema_cls

    def get(self, name: str, version: int) -> type[Schema]:
        try:
            return self._store[(name, version)]
        except KeyError:
            raise KeyError(
                f"Schema {name!r} version {version} is not registered"
            ) from None

    def migration_path(
        self,
        schema_name: str,
        from_version: int,
        to_version: int,
    ) -> list[int]:
        """Return the ordered list of version numbers to traverse from
        *from_version* to *to_version* (exclusive of *from_version*,
        inclusive of *to_version*).

        Raises ``ValueError`` if no continuous chain exists.
        """
        if from_version == to_version:
            return []
        if from_version > to_version:
            raise ValueError(
                f"Downgrade path ({from_version} → {to_version}) not supported"
            )
        path: list[int] = []
        current = from_version
        while current < to_version:
            next_version = current + 1
            if (schema_name, next_version) not in self._store:
                raise ValueError(
                    f"No schema {schema_name!r} version {next_version} registered; "
                    f"cannot build migration path {from_version} → {to_version}"
                )
            path.append(next_version)
            current = next_version
        return path


schema_registry = SchemaRegistry()
