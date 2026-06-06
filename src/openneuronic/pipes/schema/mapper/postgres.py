from __future__ import annotations

from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType


def map_field(name: str, f: Field) -> str:
    """Return the PostgreSQL column DDL fragment for an abstract *Field*."""
    sql_type = _type(f)
    null_clause = "" if f.nullable else " NOT NULL"
    return f'"{name}" {sql_type}{null_clause}'


def _type(f: Field) -> str:  # noqa: PLR0911
    match f.type:
        case FieldType.INTEGER:
            return "INTEGER"
        case FieldType.BIGINT:
            return "BIGINT"
        case FieldType.SMALLINT:
            return "SMALLINT"
        case FieldType.DECIMAL:
            if f.precision is not None and f.scale is not None:
                return f"NUMERIC({f.precision},{f.scale})"
            return "NUMERIC"
        case FieldType.FLOAT:
            return "DOUBLE PRECISION"
        case FieldType.BOOLEAN:
            return "BOOLEAN"
        case FieldType.STRING:
            if f.max_length is not None:
                return f"VARCHAR({f.max_length})"
            return "TEXT"
        case FieldType.TEXT:
            return "TEXT"
        case FieldType.CHAR:
            length = f.max_length or 1
            return f"CHAR({length})"
        case FieldType.DATE:
            return "DATE"
        case FieldType.TIME:
            return "TIME"
        case FieldType.DATETIME:
            return "TIMESTAMP"
        case FieldType.DATETIME_TZ:
            return "TIMESTAMPTZ"
        case FieldType.DURATION:
            return "INTERVAL"
        case FieldType.UUID:
            return "UUID"
        case FieldType.JSON:
            return "JSONB"
        case FieldType.BYTES:
            return "BYTEA"
        case FieldType.ARRAY:
            item = f.array_item_type
            if item is not None:
                inner = _type(Field(type=item))
                return f"{inner}[]"
            return "TEXT[]"
        case FieldType.ROWVERSION:
            # PostgreSQL has no native rowversion; use BYTEA as a compatible store.
            return "BYTEA"
        case FieldType.SERIAL:
            return "SERIAL"
        case _:
            raise ValueError(f"Unsupported FieldType for PostgreSQL: {f.type!r}")


def create_table_ddl(
    table_name: str,
    schema_cls: type,
    if_not_exists: bool = True,
) -> str:
    """Generate a ``CREATE TABLE`` statement from a :class:`~openneuronic.pipes.schema.base.Schema`."""
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    col_defs = [map_field(name, f) for name, f in schema_cls.__schema_fields__.items()]
    pk_fields = [name for name, f in schema_cls.__schema_fields__.items() if f.primary_key]
    if pk_fields:
        pk_cols = ", ".join(f'"{c}"' for c in pk_fields)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")
    cols = ",\n    ".join(col_defs)
    return f'CREATE TABLE {exists_clause}"{table_name}" (\n    {cols}\n);'
