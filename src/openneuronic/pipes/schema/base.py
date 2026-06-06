from __future__ import annotations

from typing import Any, ClassVar

from openneuronic.pipes.schema.field import Field


class SchemaMeta(type):
    """Metaclass that collects :class:`~openneuronic.pipes.schema.field.Field`
    instances declared as class attributes and exposes them via
    ``__schema_fields__``."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> SchemaMeta:
        fields: dict[str, Field] = {}

        # Inherit fields from base schemas (earlier in MRO wins for duplicates).
        for base in reversed(bases):
            if hasattr(base, "__schema_fields__"):
                fields.update(base.__schema_fields__)

        # Collect fields declared directly on this class.
        for attr, value in namespace.items():
            if isinstance(value, Field):
                fields[attr] = value

        namespace["__schema_fields__"] = fields
        return super().__new__(mcs, name, bases, namespace)


class Schema(metaclass=SchemaMeta):
    """Base class for all abstract schema definitions.

    Subclass and declare :class:`~openneuronic.pipes.schema.field.Field`
    instances as class attributes::

        @schema_version(1)
        class OrderSchemaV1(Schema):
            id         = Field(FieldType.UUID,    nullable=False, primary_key=True)
            created_at = Field(FieldType.DATETIME_TZ, nullable=False)

    The :func:`schema_version` decorator attaches ``__schema_version__`` and
    registers the class with the global :data:`~openneuronic.pipes.schema.registry.schema_registry`.
    """

    __schema_version__: ClassVar[int]
    __schema_fields__: ClassVar[dict[str, Field]]
    __schema_previous__: ClassVar[type[Schema] | None]

    @classmethod
    def family_name(cls) -> str:
        """Return the root schema name shared by all versions in this lineage."""
        from openneuronic.pipes.schema.registry import _family_name
        return _family_name(cls)

    @classmethod
    def fields(cls) -> dict[str, Field]:
        return cls.__schema_fields__

    @classmethod
    def primary_key_fields(cls) -> list[str]:
        return [name for name, f in cls.__schema_fields__.items() if f.primary_key]

    def validate(self, payload: dict[str, Any]) -> list[str]:
        """Return a list of validation error messages (empty means valid)."""
        errors: list[str] = []
        for name, f in self.__schema_fields__.items():
            if name not in payload:
                if not f.nullable and not f.has_default:
                    errors.append(f"Missing required field: {name!r}")
                continue
            value = payload[name]
            if value is None and not f.nullable:
                errors.append(f"Field {name!r} is not nullable but received None")
            if f.max_length is not None and isinstance(value, str) and len(value) > f.max_length:
                errors.append(
                    f"Field {name!r} exceeds max_length {f.max_length} (got {len(value)})"
                )
        return errors


def schema_version(
    version: int,
    previous: type[Schema] | None = None,
) -> Any:
    """Class decorator that stamps a version number onto a :class:`Schema`
    subclass and registers it with the global schema registry.

    Usage::

        @schema_version(2, previous=OrderSchemaV1)
        class OrderSchemaV2(OrderSchemaV1):
            new_field = Field(FieldType.STRING, nullable=True)
    """
    from openneuronic.pipes.schema.registry import schema_registry  # avoid circular import

    def decorator(cls: type[Schema]) -> type[Schema]:
        cls.__schema_version__ = version
        cls.__schema_previous__ = previous
        schema_registry.register(cls)
        return cls

    return decorator
