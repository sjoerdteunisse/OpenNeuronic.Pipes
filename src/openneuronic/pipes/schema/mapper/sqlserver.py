from __future__ import annotations

from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType


def map_field(name: str, f: Field) -> str:
    """Return the SQL Server column DDL fragment for an abstract *Field*."""
    sql_type = _type(f)
    null_clause = " NULL" if f.nullable else " NOT NULL"
    return f"[{name}] {sql_type}{null_clause}"


def _type(f: Field) -> str:  # noqa: PLR0911
    match f.type:
        case FieldType.INTEGER:
            return "INT"
        case FieldType.BIGINT:
            return "BIGINT"
        case FieldType.SMALLINT:
            return "SMALLINT"
        case FieldType.DECIMAL:
            if f.precision is not None and f.scale is not None:
                return f"DECIMAL({f.precision},{f.scale})"
            return "DECIMAL(18,4)"
        case FieldType.FLOAT:
            return "FLOAT"
        case FieldType.BOOLEAN:
            return "BIT"
        case FieldType.STRING:
            if f.max_length is not None:
                return f"NVARCHAR({f.max_length})"
            return "NVARCHAR(MAX)"
        case FieldType.TEXT:
            return "NVARCHAR(MAX)"
        case FieldType.CHAR:
            length = f.max_length or 1
            return f"NCHAR({length})"
        case FieldType.DATE:
            return "DATE"
        case FieldType.TIME:
            return "TIME(7)"
        case FieldType.DATETIME:
            return "DATETIME2(7)"
        case FieldType.DATETIME_TZ:
            return "DATETIMEOFFSET(7)"
        case FieldType.DURATION:
            # SQL Server has no native interval; store as seconds in BIGINT.
            return "BIGINT"
        case FieldType.UUID:
            return "UNIQUEIDENTIFIER"
        case FieldType.JSON:
            return "NVARCHAR(MAX)"  # paired with ISJSON constraint at deploy time
        case FieldType.BYTES:
            return "VARBINARY(MAX)"
        case FieldType.ARRAY:
            # SQL Server has no native array; serialise as JSON string.
            return "NVARCHAR(MAX)"
        case FieldType.ROWVERSION:
            return "ROWVERSION"
        case FieldType.SERIAL:
            return "INT IDENTITY(1,1)"
        case _:
            raise ValueError(f"Unsupported FieldType for SQL Server: {f.type!r}")


def create_table_ddl(
    table_name: str,
    schema_cls: type,
    if_not_exists: bool = True,
) -> str:
    """Generate a ``CREATE TABLE`` statement from a :class:`~openneuronic.pipes.schema.base.Schema`."""
    exists_guard = (
        f"IF OBJECT_ID(N'[dbo].[{table_name}]', N'U') IS NULL\n" if if_not_exists else ""
    )
    col_defs = [map_field(name, f) for name, f in schema_cls.__schema_fields__.items()]
    pk_fields = [name for name, f in schema_cls.__schema_fields__.items() if f.primary_key]
    if pk_fields:
        pk_cols = ", ".join(f"[{c}]" for c in pk_fields)
        col_defs.append(f"CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_cols})")
    cols = ",\n    ".join(col_defs)
    return f"{exists_guard}CREATE TABLE [dbo].[{table_name}] (\n    {cols}\n);"
