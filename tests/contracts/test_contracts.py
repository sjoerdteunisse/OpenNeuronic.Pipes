from __future__ import annotations

import pytest

from openneuronic.pipes.contracts.base import CompatibilityMode, Contract, contract_version
from openneuronic.pipes.contracts.compatibility import (
    SchemaVersionCompatibility,
    check_schema_version_compatibility,
)
from openneuronic.pipes.contracts.enforcement import (
    ContractEnforcer,
    ContractViolation,
    EnforcementStage,
)
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType


# ---------------------------------------------------------------------------
# Shared test schemas and contracts (local to this module)
# ---------------------------------------------------------------------------


@schema_version(1)
class _ItemSchemaV1(Schema):
    id       = Field(FieldType.UUID,    nullable=False, primary_key=True)
    name     = Field(FieldType.STRING,  max_length=200, nullable=False)
    price    = Field(FieldType.DECIMAL, precision=10, scale=2, nullable=False)
    currency = Field(FieldType.STRING,  max_length=3, nullable=False)


@contract_version(1)
class _ItemContractV1(Contract):
    schema          = _ItemSchemaV1
    primary_key     = ["id"]
    required_fields = ["id", "name", "price", "currency"]
    compatibility   = CompatibilityMode.FORWARD
    owner           = "test-team"


@contract_version(2)
class _StrictItemContract(Contract):
    schema          = _ItemSchemaV1
    primary_key     = ["id"]
    required_fields = ["id", "name"]
    compatibility   = CompatibilityMode.STRICT
    owner           = "test-team"


# A deliberately broken contract (no primary_key, no schema).
@contract_version(99)
class _BrokenContract(Contract):
    schema      = None
    primary_key = []
    required_fields = ["ghost_field"]
    compatibility = CompatibilityMode.STRICT
    owner       = ""


# ---------------------------------------------------------------------------
# CompatibilityMode checks
# ---------------------------------------------------------------------------


def test_exact_version_always_compatible() -> None:
    compat = check_schema_version_compatibility(_ItemContractV1, 1)
    assert compat == SchemaVersionCompatibility.COMPATIBLE


def test_forward_mode_accepts_older_version() -> None:
    # FORWARD: contract can upgrade older records.
    compat = check_schema_version_compatibility(_ItemContractV1, 0)
    assert compat == SchemaVersionCompatibility.UPGRADEABLE


def test_forward_mode_rejects_newer_version() -> None:
    compat = check_schema_version_compatibility(_ItemContractV1, 5)
    assert compat == SchemaVersionCompatibility.INCOMPATIBLE


def test_strict_mode_rejects_different_version() -> None:
    compat = check_schema_version_compatibility(_StrictItemContract, 0)
    assert compat == SchemaVersionCompatibility.INCOMPATIBLE

    compat2 = check_schema_version_compatibility(_StrictItemContract, 2)
    assert compat2 == SchemaVersionCompatibility.INCOMPATIBLE


def test_backward_mode_accepts_newer_version() -> None:
    @contract_version(100)
    class _BackwardContract(Contract):
        schema        = _ItemSchemaV1
        primary_key   = ["id"]
        compatibility = CompatibilityMode.BACKWARD
        owner         = "test"

    compat = check_schema_version_compatibility(_BackwardContract, 5)
    assert compat == SchemaVersionCompatibility.UPGRADEABLE


def test_full_mode_accepts_any_version() -> None:
    @contract_version(101)
    class _FullContract(Contract):
        schema        = _ItemSchemaV1
        primary_key   = ["id"]
        compatibility = CompatibilityMode.FULL
        owner         = "test"

    assert check_schema_version_compatibility(_FullContract, 0) == SchemaVersionCompatibility.UPGRADEABLE
    assert check_schema_version_compatibility(_FullContract, 999) == SchemaVersionCompatibility.UPGRADEABLE


# ---------------------------------------------------------------------------
# ContractEnforcer.enforce_deploy
# ---------------------------------------------------------------------------


def test_deploy_passes_valid_contract() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_ItemContractV1)
    assert result.passed


def test_deploy_fails_no_schema() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_BrokenContract)
    assert not result.passed
    messages = " ".join(v.message for v in result.violations)
    assert "schema" in messages.lower()


def test_deploy_fails_no_primary_key() -> None:
    @contract_version(200)
    class _NoPK(Contract):
        schema        = _ItemSchemaV1
        primary_key   = []
        required_fields = []
        compatibility = CompatibilityMode.STRICT
        owner         = "x"

    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_NoPK)
    assert not result.passed
    assert any("primary_key" in v.message.lower() for v in result.violations)


def test_deploy_fails_unknown_primary_key_field() -> None:
    @contract_version(201)
    class _BadPK(Contract):
        schema        = _ItemSchemaV1
        primary_key   = ["nonexistent_field"]
        required_fields = []
        compatibility = CompatibilityMode.STRICT
        owner         = "x"

    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_BadPK)
    assert not result.passed
    assert any("nonexistent_field" in v.message for v in result.violations)


# ---------------------------------------------------------------------------
# ContractEnforcer.enforce_run_start
# ---------------------------------------------------------------------------


def test_run_start_compatible_version_passes() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_run_start(_ItemContractV1, incoming_schema_version=1)
    assert result.passed


def test_run_start_incompatible_version_fails() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_run_start(_StrictItemContract, incoming_schema_version=99)
    assert not result.passed


# ---------------------------------------------------------------------------
# ContractEnforcer.enforce_pre_load
# ---------------------------------------------------------------------------


def _make_record(payload: dict) -> Record:
    r = Record(payload=payload)
    return r


def test_pre_load_valid_records_pass() -> None:
    enforcer = ContractEnforcer()
    records = [
        _make_record({"id": "a" * 36, "name": "Widget", "price": 9.99, "currency": "EUR"}),
        _make_record({"id": "b" * 36, "name": "Gadget", "price": 4.99, "currency": "USD"}),
    ]
    result = enforcer.enforce_pre_load(_ItemContractV1, records)
    assert result.passed


def test_pre_load_missing_required_field_fails() -> None:
    enforcer = ContractEnforcer()
    records = [_make_record({"id": "a" * 36, "name": "Widget"})]  # missing price + currency
    result = enforcer.enforce_pre_load(_ItemContractV1, records)
    assert not result.passed
    fields_reported = {v.field for v in result.violations}
    assert "price" in fields_reported or any("price" in v.message for v in result.violations)


def test_pre_load_empty_records_passes() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_pre_load(_ItemContractV1, [])
    assert result.passed


# ---------------------------------------------------------------------------
# ContractEnforcer.enforce_publish
# ---------------------------------------------------------------------------


def test_publish_within_deviation_passes() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_publish(_ItemContractV1, records_written=950, records_expected=1000)
    assert result.passed  # 5% deviation, contract has no limit defined


def test_publish_exceeds_deviation_fails() -> None:
    @contract_version(300)
    class _TightContract(Contract):
        schema                      = _ItemSchemaV1
        primary_key                 = ["id"]
        required_fields             = []
        compatibility               = CompatibilityMode.STRICT
        max_row_count_deviation_pct = 0.05
        owner                       = "x"

    enforcer = ContractEnforcer()
    # 40% deviation > 5% threshold
    result = enforcer.enforce_publish(_TightContract, records_written=600, records_expected=1000)
    assert not result.passed
    assert any("deviation" in v.message.lower() for v in result.violations)


def test_publish_no_expected_count_always_passes() -> None:
    @contract_version(301)
    class _TightContract2(Contract):
        schema                      = _ItemSchemaV1
        primary_key                 = ["id"]
        required_fields             = []
        compatibility               = CompatibilityMode.STRICT
        max_row_count_deviation_pct = 0.01
        owner                       = "x"

    enforcer = ContractEnforcer()
    result = enforcer.enforce_publish(_TightContract2, records_written=1)
    assert result.passed
