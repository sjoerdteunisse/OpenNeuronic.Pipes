from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.schema.base import Schema


class CompatibilityMode(StrEnum):
    STRICT   = "strict"
    FORWARD  = "forward"
    BACKWARD = "backward"
    FULL     = "full"


class ContractMeta(type):
    """Metaclass that validates required class-level attributes on Contract
    subclasses."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> ContractMeta:
        return super().__new__(mcs, name, bases, namespace)


class Contract(metaclass=ContractMeta):
    """Base class for all data contracts.

    A contract is a named, versioned set of guarantees a producer makes to
    consumers beyond what the schema alone expresses.

    Subclass and override class attributes::

        @contract_version(2)
        class OrderContractV2(Contract):
            schema                    = OrderSchemaV3
            primary_key               = ["id"]
            freshness_sla             = "30m"
            required_fields           = ["id", "tenant_id", "created_at", "currency"]
            max_row_count_deviation_pct = 0.10
            compatibility             = CompatibilityMode.FORWARD
            owner                     = "data-platform"
            required_guard_suites     = ["orders-quality-core"]
            required_measure_sets     = ["core-runtime", "dq-basic"]
    """

    __contract_version__: ClassVar[int]

    # Required overrides
    schema: ClassVar[type[Schema] | None] = None
    primary_key: ClassVar[list[str]] = []
    required_fields: ClassVar[list[str]] = []
    compatibility: ClassVar[CompatibilityMode] = CompatibilityMode.STRICT
    owner: ClassVar[str] = ""

    # Optional overrides
    freshness_sla: ClassVar[str | None] = None
    max_row_count_deviation_pct: ClassVar[float | None] = None
    required_guard_suites: ClassVar[list[str]] = []
    required_measure_sets: ClassVar[list[str]] = []
    accepted_schema_versions: ClassVar[list[int]] = []

    @classmethod
    def accepted_versions(cls) -> list[int]:
        """Schema versions this contract accepts.  If empty the contract only
        accepts the version declared on ``schema``."""
        if cls.accepted_schema_versions:
            return cls.accepted_schema_versions
        if cls.schema is not None:
            return [cls.schema.__schema_version__]
        return []


def contract_version(version: int) -> Any:
    """Class decorator that stamps a version onto a :class:`Contract` and
    registers it with the global contract registry."""
    from openneuronic.pipes.contracts.registry import contract_registry

    def decorator(cls: type[Contract]) -> type[Contract]:
        cls.__contract_version__ = version
        contract_registry.register(cls)
        return cls

    return decorator
