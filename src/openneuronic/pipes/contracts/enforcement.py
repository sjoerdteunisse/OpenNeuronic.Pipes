from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TYPE_CHECKING

from openneuronic.pipes.contracts.compatibility import (
    SchemaVersionCompatibility,
    check_schema_version_compatibility,
)

if TYPE_CHECKING:
    from openneuronic.pipes.contracts.base import Contract
    from openneuronic.pipes.core.record import Record


class EnforcementStage(StrEnum):
    DEPLOY   = "deploy"    # Validate static contract configuration
    RUN_START = "run_start" # Validate before a run begins
    PRE_LOAD  = "pre_load"  # Validate transformed records before sink write
    PUBLISH  = "publish"   # Validate quality/freshness guarantees at completion


@dataclass
class ContractViolation:
    stage: EnforcementStage
    message: str
    field: str | None = None


@dataclass
class EnforcementResult:
    stage: EnforcementStage
    contract_name: str
    contract_version: int
    violations: list[ContractViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0


class ContractEnforcer:
    """Validates a :class:`~openneuronic.pipes.contracts.base.Contract` at
    each lifecycle stage."""

    def enforce_deploy(self, contract: type[Contract]) -> EnforcementResult:
        """Static configuration checks run once at deploy/start time."""
        result = self._result(EnforcementStage.DEPLOY, contract)

        if contract.schema is None:
            result.violations.append(ContractViolation(
                stage=EnforcementStage.DEPLOY,
                message="Contract.schema must reference a Schema class",
            ))

        if not contract.primary_key:
            result.violations.append(ContractViolation(
                stage=EnforcementStage.DEPLOY,
                message="Contract.primary_key must declare at least one key field",
            ))
        elif contract.schema is not None:
            schema_fields = contract.schema.__schema_fields__
            for k in contract.primary_key:
                if k not in schema_fields:
                    result.violations.append(ContractViolation(
                        stage=EnforcementStage.DEPLOY,
                        field=k,
                        message=f"Primary key field {k!r} not found in schema",
                    ))

        if contract.schema is not None:
            schema_fields = contract.schema.__schema_fields__
            for rf in contract.required_fields:
                if rf not in schema_fields:
                    result.violations.append(ContractViolation(
                        stage=EnforcementStage.DEPLOY,
                        field=rf,
                        message=f"Required field {rf!r} not found in schema",
                    ))

        return result

    def enforce_run_start(
        self,
        contract: type[Contract],
        incoming_schema_version: int,
    ) -> EnforcementResult:
        """Check schema version compatibility before a run begins."""
        result = self._result(EnforcementStage.RUN_START, contract)
        compat = check_schema_version_compatibility(contract, incoming_schema_version)
        if compat == SchemaVersionCompatibility.INCOMPATIBLE:
            result.violations.append(ContractViolation(
                stage=EnforcementStage.RUN_START,
                message=(
                    f"Incoming schema version {incoming_schema_version} is incompatible "
                    f"with contract {contract.__name__} v{contract.__contract_version__} "
                    f"(compatibility={contract.compatibility!r})"
                ),
            ))
        return result

    def enforce_pre_load(
        self,
        contract: type[Contract],
        records: list[Record],
    ) -> EnforcementResult:
        """Validate transformed records before they are written to the sink."""
        result = self._result(EnforcementStage.PRE_LOAD, contract)

        if contract.schema is None:
            return result

        schema_fields = contract.schema.__schema_fields__

        for i, record in enumerate(records):
            payload = record.payload

            # Check required fields are present and non-null.
            for rf in contract.required_fields:
                if rf not in payload or payload[rf] is None:
                    result.violations.append(ContractViolation(
                        stage=EnforcementStage.PRE_LOAD,
                        field=rf,
                        message=(
                            f"Record {i}: required field {rf!r} is missing or null"
                        ),
                    ))

            # Schema field validation.
            schema_errors = contract.schema().validate(payload)
            for err in schema_errors:
                result.violations.append(ContractViolation(
                    stage=EnforcementStage.PRE_LOAD,
                    message=f"Record {i}: {err}",
                ))

        return result

    def enforce_publish(
        self,
        contract: type[Contract],
        records_written: int,
        records_expected: int | None = None,
    ) -> EnforcementResult:
        """Check quality guarantees at publication time."""
        result = self._result(EnforcementStage.PUBLISH, contract)

        if (
            contract.max_row_count_deviation_pct is not None
            and records_expected is not None
            and records_expected > 0
        ):
            deviation = abs(records_written - records_expected) / records_expected
            if deviation > contract.max_row_count_deviation_pct:
                result.violations.append(ContractViolation(
                    stage=EnforcementStage.PUBLISH,
                    message=(
                        f"Row count deviation {deviation:.1%} exceeds allowed "
                        f"{contract.max_row_count_deviation_pct:.1%} "
                        f"(expected ~{records_expected}, got {records_written})"
                    ),
                ))

        return result

    @staticmethod
    def _result(stage: EnforcementStage, contract: type[Contract]) -> EnforcementResult:
        return EnforcementResult(
            stage=stage,
            contract_name=contract.__name__,
            contract_version=contract.__contract_version__,
        )


contract_enforcer = ContractEnforcer()
