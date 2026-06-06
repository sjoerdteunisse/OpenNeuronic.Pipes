from __future__ import annotations

from enum import StrEnum


class CopyMode(StrEnum):
    FULL        = "full"
    PARTIAL     = "partial"
    INCREMENTAL = "incremental"


class BookmarkType(StrEnum):
    DATETIME   = "datetime"
    INTEGER    = "integer"
    ROWVERSION = "rowversion"
    LSN        = "lsn"
    CURSOR     = "cursor"


class CompatibilityMode(StrEnum):
    STRICT   = "strict"
    FORWARD  = "forward"
    BACKWARD = "backward"
    FULL     = "full"


class SegmentStatus(StrEnum):
    PENDING     = "pending"
    READY       = "ready"
    RUNNING     = "running"
    RETRYING    = "retrying"
    WAITING     = "waiting"
    SUCCESS     = "success"
    FAILED      = "failed"
    COMPENSATED = "compensated"
    CANCELLED   = "cancelled"


class GraphFailureMode(StrEnum):
    FAIL_FAST                    = "fail_fast"
    RETRY_SEGMENT                = "retry_segment"
    RETRY_BRANCH                 = "retry_branch"
    RETRY_OPUS                   = "retry_opus"
    CONTINUE_INDEPENDENT_BRANCHES = "continue_independent_branches"
    COMPENSATE_AND_STOP          = "compensate_and_stop"


class WaitStrategy(StrEnum):
    ALL      = "all"
    ANY      = "any"
    MAJORITY = "majority"
    NONE     = "none"


class TimeTravelMode(StrEnum):
    BOOKMARK = "bookmark"
    RUN      = "run"
    WINDOW   = "window"
    SNAPSHOT = "snapshot"


class ReplayWriteStrategy(StrEnum):
    DRY_RUN         = "dry_run"
    SHADOW_WRITE    = "shadow_write"
    STAGING_COMPARE = "staging_compare"
    REPLACE_SCOPE   = "replace_scope"
    REPLACE_FULL    = "replace_full"


class ResourceProfile(StrEnum):
    LIGHT    = "light"
    STANDARD = "standard"
    HEAVY    = "heavy"
