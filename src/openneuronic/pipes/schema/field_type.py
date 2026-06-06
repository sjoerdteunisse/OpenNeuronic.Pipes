from __future__ import annotations

from enum import StrEnum


class FieldType(StrEnum):
    INTEGER    = "integer"
    BIGINT     = "bigint"
    SMALLINT   = "smallint"
    DECIMAL    = "decimal"
    FLOAT      = "float"
    BOOLEAN    = "boolean"
    STRING     = "string"
    TEXT       = "text"
    CHAR       = "char"
    DATE       = "date"
    TIME       = "time"
    DATETIME   = "datetime"
    DATETIME_TZ = "datetime_tz"
    DURATION   = "duration"
    UUID       = "uuid"
    JSON       = "json"
    BYTES      = "bytes"
    ARRAY      = "array"
    ROWVERSION = "rowversion"
    SERIAL     = "serial"
