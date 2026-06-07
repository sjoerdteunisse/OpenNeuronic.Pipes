"""README example tests — Contracts section."""
from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes import Record
from openneuronic.pipes.contracts.base import Contract, contract_version
from openneuronic.pipes.contracts.enforcement import ContractEnforcer
from openneuronic.pipes.contracts.registry import ContractRegistry
from openneuronic.pipes.core.enums import CompatibilityMode
from openneuronic.pipes.guards.suite import GuardSuite, guard_suite_registry
from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType


# ---------------------------------------------------------------------------
# Schema and contract definitions
# ---------------------------------------------------------------------------

@schema_version(10)
class _CS1(Schema):
    id         = Field(FieldType.UUID,    nullable=False, primary_key=True)
    created_at = Field(FieldType.DATETIME_TZ, nullable=False)
    status     = Field(FieldType.STRING,  max_length=20, nullable=False, default="pending")


# Register the guard suite expected by the contract
_orders_suite = GuardSuite("readme-orders-quality-contract", guards=[])
guard_suite_registry.register(_orders_suite)


@contract_version(10)
class _OrderContract(Contract):
    schema                      = _CS1
    primary_key                 = ["id"]
    required_fields             = ["id", "created_at", "status"]
    compatibility               = CompatibilityMode.FORWARD
    owner                       = "data-platform"
    freshness_sla               = "1h"
    max_row_count_deviation_pct = 0.10
    required_guard_suites       = ["readme-orders-quality-contract"]
    accepted_schema_versions    = [10]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_enforce_deploy_passes_for_valid_contract() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_OrderContract)
    assert result.passed


def test_enforce_run_start_passes_for_accepted_version() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_run_start(_OrderContract, incoming_schema_version=10)
    assert result.passed


def test_enforce_run_start_fails_for_rejected_version() -> None:
    enforcer = ContractEnforcer()
    result = enforcer.enforce_run_start(_OrderContract, incoming_schema_version=99)
    assert not result.passed


def test_enforce_pre_load_passes_for_complete_records() -> None:
    enforcer = ContractEnforcer()
    records = [
        Record(payload={"id": "abc", "created_at": "2024-01-01", "status": "active"}),
    ]
    result = enforcer.enforce_pre_load(_OrderContract, records)
    assert result.passed


def test_enforce_pre_load_fails_when_required_field_missing() -> None:
    enforcer = ContractEnforcer()
    records = [
        Record(payload={"id": "abc"}),  # missing created_at and status
    ]
    result = enforcer.enforce_pre_load(_OrderContract, records)
    assert not result.passed


def test_enforce_publish_passes_within_sla_and_deviation() -> None:
    enforcer = ContractEnforcer()
    now  = datetime.datetime.now(datetime.UTC)
    last = now - datetime.timedelta(minutes=30)
    result = enforcer.enforce_publish(
        _OrderContract,
        records_written=1000,
        records_expected=1010,
        run_finished_at=now,
        last_successful_run_at=last,
    )
    assert result.passed


def test_enforce_publish_fails_when_freshness_sla_exceeded() -> None:
    enforcer = ContractEnforcer()
    now  = datetime.datetime.now(datetime.UTC)
    last = now - datetime.timedelta(hours=2)   # 2h > 1h SLA
    result = enforcer.enforce_publish(
        _OrderContract,
        records_written=1000,
        records_expected=1000,
        run_finished_at=now,
        last_successful_run_at=last,
    )
    assert not result.passed


def test_enforce_publish_fails_on_excessive_row_count_deviation() -> None:
    enforcer = ContractEnforcer()
    now  = datetime.datetime.now(datetime.UTC)
    last = now - datetime.timedelta(minutes=5)
    result = enforcer.enforce_publish(
        _OrderContract,
        records_written=800,
        records_expected=1000,   # 20% deviation > 10% limit
        run_finished_at=now,
        last_successful_run_at=last,
    )
    assert not result.passed


def test_contract_registry_get_latest() -> None:
    reg = ContractRegistry()

    @contract_version(20)
    class _C20(Contract):
        schema = _CS1
        accepted_schema_versions = [10]

    @contract_version(21)
    class _C21(_C20):
        pass

    reg.register(_C20)
    reg.register(_C21)

    latest = reg.get_latest("_C20")
    assert latest.__contract_version__ >= 20  # type: ignore[attr-defined]


def test_contract_freshness_sla_various_formats() -> None:
    from openneuronic.pipes.contracts.enforcement import _parse_sla
    assert _parse_sla("30m") == 1800.0
    assert _parse_sla("1h")  == 3600.0
    assert _parse_sla("2d")  == 172800.0
    assert _parse_sla("90")  == 90.0
    assert _parse_sla(None)  is None
