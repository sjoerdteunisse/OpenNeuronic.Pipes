"""ReplayPoint — the minimum reproducible execution boundary for a pipe run."""
from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReplayPoint:
    """Captures the exact state needed to reproduce or restore a past run.

    Attributes:
        pipe_id: The pipe this snapshot belongs to (``None`` for opus-level).
        source_bookmarks: Mapping of ``pipe_id → bookmark value`` at capture time.
        schema_versions: Mapping of schema class name → version number.
        contract_versions: Mapping of contract class name → version number.
        config_digest: Short SHA-256 hex digest of the pipe's configuration.
        replay_id: Unique identifier for this replay point (UUID).
        opus_id: Owning opus, when captured as part of an opus run.
        created_at: UTC-aware timestamp of capture.
        lineage_ref: Optional reference to a lineage event or run ID.
        manifest_ref: Optional reference to a :class:`ReplayManifest`.
    """

    pipe_id: str | None
    source_bookmarks: dict[str, Any]
    schema_versions: dict[str, int]
    contract_versions: dict[str, int]
    config_digest: str
    replay_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    opus_id: str | None = None
    created_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    lineage_ref: str | None = None
    manifest_ref: str | None = None
