"""Equivalence integration tests — Contracts.

Asserts that:
- enforce_deploy passes for a valid contract.
- enforce_pre_load rejects records missing required fields.
- enforce_publish fails when deviation or SLA is exceeded.
- The same contract enforcement produces identical results whether called via
  the global contract_enforcer or a fresh ContractEnforcer instance.

Run with::

    py -3.14 -m pytest tests/integration/test_contract_equivalence.py -v -m integration
"""
from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes import Record
from openneuronic.pipes.contracts.base import Contract, contract_version
from openneuronic.pipes.contracts.enforcement import ContractEnforcer, contract_enforcer
from openneuronic.pipes.core.enums import CompatibilityMode
from openneuronic.pipes.guards.suite import GuardSuite, guard_suite_registry
from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@schema_version(50)
class _EquivSchema(Schema):
    id         = Field(FieldType.UUID,        nullable=False, primary_key=True)
    name       = Field(FieldType.STRING,      max_length=200, nullable=False)
    created_at = Field(FieldType.DATETIME_TZ, nullable=False)


_suite = GuardSuite("contract-equiv-suite", guards=[])
guard_suite_registry.register(_suite)


@contract_version(50)
class _EquivContract(Contract):
    schema                      = _EquivSchema
    primary_key                 = ["id"]
    required_fields             = ["id", "name", "created_at"]
    compatibility               = CompatibilityMode.FORWARD
    owner                       = "test"
    freshness_sla               = "2h"
    max_row_count_deviation_pct = 0.20
    required_guard_suites       = ["contract-equiv-suite"]
    accepted_schema_versions    = [50]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fresh_enforcer_and_global_enforcer_agree_on_deploy() -> None:
    """Both a new ContractEnforcer and the global one pass the same valid contract."""
    fresh   = ContractEnforcer()
    r_fresh  = fresh.enforce_deploy(_EquivContract)
    r_global = contract_enforcer.enforce_deploy(_EquivContract)

    assert r_fresh.passed == r_global.passed
    assert r_fresh.passed is True


def test_enforce_deploy_passes_for_registered_guard_suite() -> None:
    """enforce_deploy passes when all required_guard_suites are registered."""
    enforcer = ContractEnforcer()
    result = enforcer.enforce_deploy(_EquivContract)
    assert result.passed


def test_enforce_pre_load_rejects_missing_required_field() -> None:
    """A record missing a required field must fail enforce_pre_load."""
    enforcer = ContractEnforcer()
    bad_records = [
        Record(payload={"id": "abc", "created_at": "2024-01-01"}),  # missing 'name'
    ]
    r_fresh  = enforcer.enforce_pre_load(_EquivContract, bad_records)
    r_global = contract_enforcer.enforce_pre_load(_EquivContract, bad_records)

    assert r_fresh.passed is False
    assert r_global.passed is False


def test_enforce_pre_load_passes_for_complete_records() -> None:
    """Records with all required fields pass both enforcer instances."""
    enforcer = ContractEnforcer()
    good_records = [
        Record(payload={"id": "abc", "name": "Alice", "created_at": "2024-01-01"}),
    ]
    r_fresh  = enforcer.enforce_pre_load(_EquivContract, good_records)
    r_global = contract_enforcer.enforce_pre_load(_EquivContract, good_records)

    assert r_fresh.passed is True
    assert r_global.passed is True


def test_enforce_publish_passes_within_limits() -> None:
    """Both enforcer instances agree that a publish within SLA and deviation passes."""
    now  = datetime.datetime.now(datetime.UTC)
    last = now - datetime.timedelta(hours=1)

    enforcer = ContractEnforcer()
    kwargs = dict(
        records_written=1000,
        records_expected=1050,
        run_finished_at=now,
        last_successful_run_at=last,
    )
    r_fresh  = enforcer.enforce_publish(_EquivContract, **kwargs)
    r_global = contract_enforcer.enforce_publish(_EquivContract, **kwargs)

    assert r_fresh.passed == r_global.passed
    assert r_fresh.passed is True


def test_enforce_publish_fails_identically_on_sla_breach() -> None:
    """Both enforcers produce identical failure on SLA breach."""
    now  = datetime.datetime.now(datetime.UTC)
    last = now - datetime.timedelta(hours=3)   # 3h > 2h SLA

    enforcer = ContractEnforcer()
    kwargs = dict(
        records_written=1000,
        records_expected=1000,
        run_finished_at=now,
        last_successful_run_at=last,
    )
    r_fresh  = enforcer.enforce_publish(_EquivContract, **kwargs)
    r_global = contract_enforcer.enforce_publish(_EquivContract, **kwargs)

    assert r_fresh.passed == r_global.passed
    assert r_fresh.passed is False
