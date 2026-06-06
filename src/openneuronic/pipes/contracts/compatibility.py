from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.contracts.base import CompatibilityMode, Contract
    from openneuronic.pipes.schema.base import Schema


class SchemaVersionCompatibility(StrEnum):
    COMPATIBLE   = "compatible"
    UPGRADEABLE  = "upgradeable"
    INCOMPATIBLE = "incompatible"


def check_schema_version_compatibility(
    contract: type[Contract],
    incoming_version: int,
) -> SchemaVersionCompatibility:
    """Determine whether *incoming_version* is compatible with *contract*.

    Rules:
    - ``STRICT``: only the exact schema version the contract declares is accepted.
    - ``FORWARD``: older records (lower version) can be upgraded → accepted.
    - ``BACKWARD``: newer records (higher version) can be downgraded → accepted.
    - ``FULL``: both older and newer versions are accepted.
    """
    from openneuronic.pipes.contracts.base import CompatibilityMode

    if contract.schema is None:
        return SchemaVersionCompatibility.COMPATIBLE

    declared = contract.schema.__schema_version__

    if incoming_version == declared:
        return SchemaVersionCompatibility.COMPATIBLE

    accepted = contract.accepted_versions()
    if incoming_version in accepted:
        return SchemaVersionCompatibility.COMPATIBLE

    mode: CompatibilityMode = contract.compatibility

    match mode:
        case CompatibilityMode.FORWARD:
            # Producer is ahead; consumer (contract) is older.  Contract accepts
            # older records that can be upgraded to its schema version.
            if incoming_version < declared:
                return SchemaVersionCompatibility.UPGRADEABLE
        case CompatibilityMode.BACKWARD:
            # Consumer is behind; it accepts newer records that the contract
            # knows how to downgrade.
            if incoming_version > declared:
                return SchemaVersionCompatibility.UPGRADEABLE
        case CompatibilityMode.FULL:
            return SchemaVersionCompatibility.UPGRADEABLE

    return SchemaVersionCompatibility.INCOMPATIBLE
