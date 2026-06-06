from __future__ import annotations

import pytest

from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.diff import ChangeKind, diff_schemas
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType
from openneuronic.pipes.schema.mapper import postgres as pg_mapper
from openneuronic.pipes.schema.mapper import sqlserver as ss_mapper
from openneuronic.pipes.schema.migration import (
    Migration,
    MigrationStep,
    MigrationStepKind,
    RuntimeUpgrader,
    migration_registry,
)
from openneuronic.pipes.schema.registry import SchemaRegistry


# ---------------------------------------------------------------------------
# Shared test schema fixtures
# ---------------------------------------------------------------------------

# Use a fresh registry per module to avoid polluting the global one.
_reg = SchemaRegistry()


class _OrderV1(Schema):
    id         = Field(FieldType.UUID,        nullable=False, primary_key=True)
    amount     = Field(FieldType.DECIMAL,     precision=18, scale=4)
    currency   = Field(FieldType.STRING,      max_length=3, nullable=False)
    created_at = Field(FieldType.DATETIME_TZ, nullable=False)


class _OrderV2(_OrderV1):
    total_amount = Field(FieldType.DECIMAL, precision=18, scale=4)
    note         = Field(FieldType.TEXT, nullable=True)


# Stamp version numbers (bypassing the global decorator to keep tests isolated).
_OrderV1.__schema_version__ = 1
_OrderV1.__schema_previous__ = None
_OrderV2.__schema_version__ = 2
_OrderV2.__schema_previous__ = _OrderV1
_reg.register(_OrderV1)
_reg.register(_OrderV2)


# ---------------------------------------------------------------------------
# Field / FieldType tests
# ---------------------------------------------------------------------------

def test_field_has_default() -> None:
    f = Field(type=FieldType.INTEGER, default=0)
    assert f.has_default

def test_field_no_default() -> None:
    f = Field(type=FieldType.STRING)
    assert not f.has_default

def test_field_primary_key_default_false() -> None:
    f = Field(type=FieldType.UUID)
    assert not f.primary_key


# ---------------------------------------------------------------------------
# Schema metaclass / field discovery
# ---------------------------------------------------------------------------

def test_schema_fields_collected() -> None:
    fields = _OrderV1.__schema_fields__
    assert "id" in fields
    assert "amount" in fields
    assert "currency" in fields
    assert "created_at" in fields


def test_schema_inheritance_merges_fields() -> None:
    fields = _OrderV2.__schema_fields__
    # Inherited from V1
    assert "id" in fields
    assert "amount" in fields
    # New in V2
    assert "total_amount" in fields
    assert "note" in fields


def test_primary_key_fields() -> None:
    assert _OrderV1.primary_key_fields() == ["id"]


def test_schema_validate_passes() -> None:
    payload = {
        "id": "a" * 36,
        "amount": 99.99,
        "currency": "EUR",
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    errors = _OrderV1().validate(payload)
    assert errors == []


def test_schema_validate_catches_null_on_non_nullable() -> None:
    payload = {
        "id": "a" * 36,
        "amount": None,
        "currency": None,   # non-nullable
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    errors = _OrderV1().validate(payload)
    assert any("currency" in e for e in errors)


def test_schema_validate_catches_max_length_violation() -> None:
    payload = {
        "id": "a" * 36,
        "amount": 1.0,
        "currency": "TOOLONG",  # max_length=3
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    errors = _OrderV1().validate(payload)
    assert any("currency" in e for e in errors)


# ---------------------------------------------------------------------------
# SchemaDiff
# ---------------------------------------------------------------------------

def test_diff_safe_add_nullable_field() -> None:
    d = diff_schemas(_OrderV1, _OrderV2)
    names = {c.name for c in d.safe}
    assert "note" in names
    assert not d.has_breaking_changes


def test_diff_breaking_drop_field() -> None:
    class _Dropped(Schema):
        id = Field(FieldType.UUID, nullable=False, primary_key=True)
        # 'amount', 'currency', 'created_at' dropped

    _Dropped.__schema_version__ = 99
    _Dropped.__schema_previous__ = None
    d = diff_schemas(_OrderV1, _Dropped)
    assert d.has_breaking_changes
    breaking_names = {c.name for c in d.breaking}
    assert "amount" in breaking_names
    assert "currency" in breaking_names


def test_diff_breaking_nullable_narrowed() -> None:
    class _Narrowed(_OrderV1):
        amount = Field(FieldType.DECIMAL, nullable=False, precision=18, scale=4)

    _Narrowed.__schema_version__ = 99
    _Narrowed.__schema_previous__ = _OrderV1
    d = diff_schemas(_OrderV1, _Narrowed)
    assert d.has_breaking_changes
    assert any(c.name == "amount" for c in d.breaking)


def test_diff_safe_widen_max_length() -> None:
    class _Widened(_OrderV1):
        currency = Field(FieldType.STRING, max_length=10, nullable=False)

    _Widened.__schema_version__ = 99
    _Widened.__schema_previous__ = _OrderV1
    d = diff_schemas(_OrderV1, _Widened)
    assert any(c.name == "currency" and c.kind == ChangeKind.SAFE for c in d.changes)


# ---------------------------------------------------------------------------
# DDL generation — PostgreSQL
# ---------------------------------------------------------------------------

def test_pg_create_table_ddl_contains_columns() -> None:
    ddl = pg_mapper.create_table_ddl("orders", _OrderV1)
    assert '"id" UUID NOT NULL' in ddl
    assert '"currency" VARCHAR(3) NOT NULL' in ddl
    assert "PRIMARY KEY" in ddl
    assert 'CREATE TABLE IF NOT EXISTS "orders"' in ddl


def test_pg_field_type_mappings() -> None:
    cases = [
        (Field(FieldType.INTEGER),           "INTEGER"),
        (Field(FieldType.BIGINT),            "BIGINT"),
        (Field(FieldType.BOOLEAN),           "BOOLEAN"),
        (Field(FieldType.UUID),              "UUID"),
        (Field(FieldType.JSON),              "JSONB"),
        (Field(FieldType.DATETIME_TZ),       "TIMESTAMPTZ"),
        (Field(FieldType.TEXT),              "TEXT"),
        (Field(FieldType.DECIMAL, precision=10, scale=2), "NUMERIC(10,2)"),
        (Field(FieldType.FLOAT),             "DOUBLE PRECISION"),
    ]
    for f, expected in cases:
        assert pg_mapper._type(f) == expected, f"Expected {expected} for {f.type!r}"


# ---------------------------------------------------------------------------
# DDL generation — SQL Server
# ---------------------------------------------------------------------------

def test_ss_create_table_ddl_contains_columns() -> None:
    ddl = ss_mapper.create_table_ddl("orders", _OrderV1)
    assert "[id] UNIQUEIDENTIFIER NOT NULL" in ddl
    assert "[currency] NVARCHAR(3) NOT NULL" in ddl
    assert "PRIMARY KEY" in ddl
    assert "CREATE TABLE [dbo].[orders]" in ddl


def test_ss_field_type_mappings() -> None:
    cases = [
        (Field(FieldType.INTEGER),           "INT"),
        (Field(FieldType.BIGINT),            "BIGINT"),
        (Field(FieldType.BOOLEAN),           "BIT"),
        (Field(FieldType.UUID),              "UNIQUEIDENTIFIER"),
        (Field(FieldType.JSON),              "NVARCHAR(MAX)"),
        (Field(FieldType.DATETIME_TZ),       "DATETIMEOFFSET(7)"),
        (Field(FieldType.TEXT),              "NVARCHAR(MAX)"),
        (Field(FieldType.DECIMAL, precision=10, scale=2), "DECIMAL(10,2)"),
        (Field(FieldType.ROWVERSION),        "ROWVERSION"),
        (Field(FieldType.SERIAL),            "INT IDENTITY(1,1)"),
    ]
    for f, expected in cases:
        assert ss_mapper._type(f) == expected, f"Expected {expected} for {f.type!r}"


# ---------------------------------------------------------------------------
# MigrationStep
# ---------------------------------------------------------------------------

def test_step_rename() -> None:
    p = {"amount": 100, "currency": "EUR"}
    result = MigrationStep.rename("amount", "total_amount").apply(p)
    assert "total_amount" in result
    assert "amount" not in result
    assert result["total_amount"] == 100


def test_step_fill_missing() -> None:
    p = {"id": "x"}
    result = MigrationStep.fill("currency", "EUR").apply(p)
    assert result["currency"] == "EUR"


def test_step_fill_preserves_existing() -> None:
    p = {"currency": "USD"}
    result = MigrationStep.fill("currency", "EUR").apply(p)
    assert result["currency"] == "USD"


def test_step_drop() -> None:
    p = {"id": "x", "legacy_field": "y"}
    result = MigrationStep.drop("legacy_field").apply(p)
    assert "legacy_field" not in result


def test_step_add_when_absent() -> None:
    p = {"id": "x"}
    result = MigrationStep.add("retry_count", 0).apply(p)
    assert result["retry_count"] == 0


def test_step_cast() -> None:
    p = {"amount": "99.99"}
    result = MigrationStep.cast("amount", float).apply(p)
    assert result["amount"] == 99.99


# ---------------------------------------------------------------------------
# Migration / RuntimeUpgrader
# ---------------------------------------------------------------------------

def test_migration_applies_steps_in_order() -> None:
    m = Migration(
        from_version=1,
        to_version=2,
        steps=[
            MigrationStep.rename("amount", "total_amount"),
            MigrationStep.fill("currency", "EUR"),
            MigrationStep.drop("amount"),
        ],
    )
    payload = {"id": "abc", "amount": 50.0}
    result = m.apply(payload)
    assert "total_amount" in result
    assert result["currency"] == "EUR"
    assert "amount" not in result


def test_upgrader_chain() -> None:
    # Build a fresh registry + upgrader so we don't pollute globals.
    reg = SchemaRegistry()

    class _V1(Schema):
        id  = Field(FieldType.UUID, nullable=False, primary_key=True)
        old = Field(FieldType.STRING, nullable=True)

    class _V2(_V1):
        renamed = Field(FieldType.STRING, nullable=True)

    class _V3(_V2):
        extra = Field(FieldType.INTEGER, nullable=True, default=0)

    _V1.__schema_version__ = 1; _V1.__schema_previous__ = None
    _V2.__schema_version__ = 2; _V2.__schema_previous__ = _V1
    _V3.__schema_version__ = 3; _V3.__schema_previous__ = _V2
    reg.register(_V1); reg.register(_V2); reg.register(_V3)

    m1_to_2 = Migration(
        from_version=1, to_version=2,
        steps=[MigrationStep.rename("old", "renamed")],
    )
    m2_to_3 = Migration(
        from_version=2, to_version=3,
        steps=[MigrationStep.add("extra", 0)],
    )
    local_mig_reg = type("_Reg", (), {
        "get": lambda self, name, fv, tv: m1_to_2 if (fv, tv) == (1, 2) else m2_to_3
    })()

    upgrader = RuntimeUpgrader()
    # Patch registries locally.
    import openneuronic.pipes.schema.migration as _mig_mod
    import openneuronic.pipes.schema.registry as _reg_mod
    original_reg = _reg_mod.schema_registry
    original_mig = _mig_mod.migration_registry
    _reg_mod.schema_registry = reg
    _mig_mod.migration_registry = local_mig_reg

    try:
        payload = {"id": "abc", "old": "hello"}
        result = upgrader.upgrade(payload, "_V1", from_version=1, to_version=3)
    finally:
        _reg_mod.schema_registry = original_reg
        _mig_mod.migration_registry = original_mig

    assert "renamed" in result
    assert "old" not in result
    assert result["extra"] == 0


def test_upgrader_noop_same_version() -> None:
    upgrader = RuntimeUpgrader()
    payload = {"id": "abc"}
    result = upgrader.upgrade(payload, "SomeSchema", from_version=2, to_version=2)
    assert result == payload
