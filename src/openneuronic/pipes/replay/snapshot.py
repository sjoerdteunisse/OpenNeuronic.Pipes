"""Snapshot helpers — create a ReplayPoint from a completed run."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from openneuronic.pipes.core.pipe import Pipe
    from openneuronic.pipes.runner import RunResult

from openneuronic.pipes.replay.point import ReplayPoint


def _config_digest(pipe: Pipe) -> str:
    """Return a 16-hex-character SHA-256 digest of the pipe's static config."""
    data: dict[str, Any] = {
        "pipe_id": pipe.id,
        "mode": str(pipe.mode),
        "source": type(pipe.source).__name__,
        "sink": type(pipe.sink).__name__,
        "processors": [type(p).__name__ for p in pipe.processors],
    }
    serialised = json.dumps(data, sort_keys=True).encode()
    return hashlib.sha256(serialised).hexdigest()[:16]


def create_snapshot(
    pipe: Pipe,
    run_result: RunResult,
    bookmark_value: Any = None,
) -> ReplayPoint:
    """Build a :class:`~openneuronic.pipes.replay.point.ReplayPoint` from a
    completed run.

    Args:
        pipe: The pipe that was executed.
        run_result: The :class:`~openneuronic.pipes.runner.RunResult` produced.
        bookmark_value: Optional bookmark value to store in
            ``source_bookmarks``.  When ``None`` no bookmark entry is added.

    Returns:
        A :class:`ReplayPoint` with a stable ``config_digest`` and the active
        schema/contract versions extracted from the pipe.
    """
    schema_versions: dict[str, int] = {}
    if pipe.schema is not None and hasattr(pipe.schema, "__schema_version__"):
        schema_versions[
            getattr(pipe.schema, "__name__", "schema")
        ] = pipe.schema.__schema_version__

    contract_versions: dict[str, int] = {}
    if pipe.contract is not None and hasattr(pipe.contract, "__contract_version__"):
        contract_versions[
            getattr(pipe.contract, "__name__", "contract")
        ] = pipe.contract.__contract_version__

    source_bookmarks: dict[str, Any] = {}
    if bookmark_value is not None:
        source_bookmarks[pipe.id] = bookmark_value

    lineage_ref: str | None = None
    if hasattr(run_result, "run_id"):
        lineage_ref = run_result.run_id

    return ReplayPoint(
        pipe_id=pipe.id,
        source_bookmarks=source_bookmarks,
        schema_versions=schema_versions,
        contract_versions=contract_versions,
        config_digest=_config_digest(pipe),
        lineage_ref=lineage_ref,
    )
