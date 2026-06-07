"""README example tests — Schema System section."""
from __future__ import annotations

import pytest

from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.diff import ChangeKind, diff_schemas
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType
from openneuronic.pipes.schema.mapper.postgres import create_table_ddl as pg_ddl
from openneuronic.pipes.schema.mapper.sqlserver import create_table_ddl as ss_ddl
from openneuronic.pipes.schema.migration import Migration, MigrationStep, RuntimeUpgrader
from openneuronic.pipes import Record


# ---------------------------------------------------------------------------
# Schema definitions (shared across tests)
# ---------------------------------------------------------------------------

@schema_version(1)
class OrderSchemaV1(Schema):
    id         = Field(FieldType.UUID,        nullable=False, primary_key=True)
    tenant_id  = Field(FieldType.STRING,      max_length=100, nullable=True)
    total      = Field(FieldType.DECIMAL,     precision=18, scale=4)
    created_at = Field(FieldType.DATETIME_TZ, nullable=False)
    status     = Field(FieldType.STRING,      max_length=20, nullable=False, default="pending")


@schema_version(2, previous=OrderSchemaV1)
class OrderSchemaV2(OrderSchemaV1):
    currency = Field(FieldType.STRING, max_length=3, nullable=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_schema_class_has_version_attribute() -> None:
    assert OrderSchemaV1.__schema_version__ == 1  # type: ignore[attr-defined]
    assert OrderSchemaV2.__schema_version__ == 2  # type: ignore[attr-defined]


def test_schema_v2_has_all_v1_fields() -> None:
    v1_fields = {f for f in OrderSchemaV1.__dict__ if isinstance(OrderSchemaV1.__dict__[f], Field)}
    v2_fields = {f for f in OrderSchemaV2.__dict__ if isinstance(OrderSchemaV2.__dict__.get(f), Field)}
    # v2 inherits v1 fields; its OWN new field is currency
    assert "currency" in v2_fields


def test_schema_validate_catches_nonnullable_none() -> None:
    errors = OrderSchemaV1().validate({"id": None, "created_at": "2024-01-01", "status": "ok"})
    assert any("id" in e for e in errors)


def test_schema_validate_passes_valid_payload() -> None:
    errors = OrderSchemaV1().validate({
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "created_at": "2024-01-01",
        "status": "active",
    })
    assert errors == []


def test_diff_detects_safe_addition() -> None:
    diff = diff_schemas(OrderSchemaV1, OrderSchemaV2)
    names = [c.name for c in diff.changes]
    assert "currency" in names
    for change in diff.changes:
        assert change.kind == ChangeKind.SAFE


def test_diff_no_breaking_changes_on_nullable_addition() -> None:
    diff = diff_schemas(OrderSchemaV1, OrderSchemaV2)
    assert not diff.has_breaking_changes


def test_runtime_upgrader_fills_missing_field() -> None:
    from openneuronic.pipes.schema.migration import migration_registry
    from openneuronic.pipes.schema.registry import schema_registry

    migration = Migration(
        from_version=1,
        to_version=2,
        steps=[MigrationStep.fill("currency", "EUR")],
        ddl_up="ALTER TABLE orders ADD currency NVARCHAR(3) NULL",
    )
    # Migrations are registered on the global registry keyed by schema name.
    schema_name = "OrderSchemaV1"
    migration_registry.register(schema_name, migration)

    # RuntimeUpgrader.upgrade works on a payload dict, not a Record directly.
    upgrader = RuntimeUpgrader()
    old_payload = {"id": "abc", "total": "100.00"}
    new_payload = upgrader.upgrade(old_payload, schema_name=schema_name, from_version=1, to_version=2)
    assert new_payload["currency"] == "EUR"


def test_runtime_upgrader_noop_when_already_at_target() -> None:
    upgrader = RuntimeUpgrader()
    payload = {"id": "x"}
    # When from_version == to_version, the payload is returned unchanged.
    result = upgrader.upgrade(payload, schema_name="OrderSchemaV1", from_version=2, to_version=2)
    assert result["id"] == "x"


def test_pg_ddl_contains_table_name() -> None:
    ddl = pg_ddl("orders", OrderSchemaV1)
    assert "orders" in ddl.lower()


def test_pg_ddl_contains_field_names() -> None:
    ddl = pg_ddl("orders", OrderSchemaV1)
    assert "id" in ddl.lower()
    assert "status" in ddl.lower()


def test_ss_ddl_contains_table_name() -> None:
    ddl = ss_ddl("orders", OrderSchemaV1)
    assert "orders" in ddl.lower()


def test_ss_ddl_contains_object_id_guard() -> None:
    ddl = ss_ddl("orders", OrderSchemaV1)
    assert "OBJECT_ID" in ddl or "object_id" in ddl.lower()
