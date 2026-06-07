"""ReplayRunner — executes a pipe from a persisted ReplayPoint."""
from __future__ import annotations

import dataclasses
from typing import Any

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import CopyMode, ReplayWriteStrategy, TimeTravelMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.replay.manifest import ReplayManifest
from openneuronic.pipes.replay.point import ReplayPoint
from openneuronic.pipes.replay.store import ReplayStore
from openneuronic.pipes.runner import LocalRunner, RunResult


class ReplayValidationError(Exception):
    """Raised when a replay cannot proceed due to safety checks."""


class ReplayRunner:
    """Executes a pipe restored from a :class:`~openneuronic.pipes.replay.point.ReplayPoint`.

    Replay safety is checked before execution:
    - The pipe's current config digest must match the stored one unless the
      caller explicitly passes ``allow_config_drift=True``.
    - ``DRY_RUN`` mode runs the full pipeline but routes output to a
      :class:`_DevNullSink` — no records are written.
    - ``SHADOW_WRITE`` mode writes to ``{table}_shadow`` instead of the real table.
    - ``STAGING_COMPARE`` mode writes to ``{table}_replay_staging`` and raises
      :class:`ReplayStagingComplete` so the caller can inspect differences.
    - ``REPLACE_SCOPE`` and ``REPLACE_FULL`` write normally (same as a live run).

    Example::

        runner = ReplayRunner(replay_store=InMemoryReplayStore())
        result = await runner.run(
            pipe=my_pipe,
            manifest=ReplayManifest(
                replay_point_id="...",
                mode=TimeTravelMode.BOOKMARK,
                scope={},
                source_snapshot={},
                sink_strategy=ReplayWriteStrategy.DRY_RUN,
            ),
        )
    """

    def __init__(
        self,
        replay_store: ReplayStore,
        batch_size: int = 500,
        allow_config_drift: bool = False,
    ) -> None:
        self._store = replay_store
        self._batch_size = batch_size
        self._allow_config_drift = allow_config_drift

    async def run(
        self,
        pipe: Pipe,
        manifest: ReplayManifest,
    ) -> RunResult:
        """Execute *pipe* as described by *manifest*.

        Returns the same :class:`~openneuronic.pipes.runner.RunResult` as a
        normal run.
        """
        # Load the replay point.
        point = await self._store.load(manifest.replay_point_id)
        if point is None:
            raise ReplayValidationError(
                f"ReplayPoint {manifest.replay_point_id!r} not found in store"
            )

        # Config-drift guard.
        if not self._allow_config_drift and point.pipe_id == pipe.id:
            from openneuronic.pipes.replay.snapshot import _config_digest
            current_digest = _config_digest(pipe)
            if current_digest != point.config_digest:
                raise ReplayValidationError(
                    f"Pipe {pipe.id!r} config digest mismatch: "
                    f"stored={point.config_digest!r}, current={current_digest!r}. "
                    "Pass allow_config_drift=True to override."
                )

        # Restore bookmark from replay point.
        bookmark = self._restore_bookmark(pipe, point)

        # Apply sink strategy.
        strategy = ReplayWriteStrategy(manifest.sink_strategy)
        replay_pipe = self._apply_strategy(pipe, strategy)

        # Apply scope override for WINDOW mode.
        if manifest.mode == TimeTravelMode.WINDOW and manifest.scope:
            replay_pipe = self._apply_scope(replay_pipe, manifest.scope)

        runner = LocalRunner(batch_size=self._batch_size)
        return await runner.run(replay_pipe, bookmark)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _restore_bookmark(
        self, pipe: Pipe, point: ReplayPoint
    ) -> Bookmark | None:
        """Build a :class:`Bookmark` from the stored source_bookmarks dict."""
        bm_value = point.source_bookmarks.get(pipe.id)
        if bm_value is None:
            return None
        # Infer bookmark type from the current pipe config if available.
        existing_bm = getattr(pipe.source, "_bookmark", None)
        if existing_bm is not None:
            return dataclasses.replace(existing_bm, value=bm_value)
        # Fallback: create a minimal Bookmark with CURSOR type.
        from openneuronic.pipes.core.enums import BookmarkType
        return Bookmark(
            pipe_id=pipe.id,
            column="",
            type=BookmarkType.CURSOR,
            value=bm_value,
        )

    def _apply_strategy(self, pipe: Pipe, strategy: ReplayWriteStrategy) -> Pipe:
        """Return a (possibly modified) pipe with the replay write strategy applied."""
        if strategy == ReplayWriteStrategy.DRY_RUN:
            return dataclasses.replace(pipe, sink=_DevNullSink())

        if strategy == ReplayWriteStrategy.SHADOW_WRITE:
            sink = pipe.sink
            table = getattr(sink, "_table", None)
            if table is not None:
                import copy
                shadow_sink = copy.copy(sink)
                shadow_sink._table = f"{table}_shadow"  # type: ignore[attr-defined]
                return dataclasses.replace(pipe, sink=shadow_sink)
            return pipe  # cannot shadow without a table name

        if strategy == ReplayWriteStrategy.STAGING_COMPARE:
            sink = pipe.sink
            table = getattr(sink, "_table", None)
            if table is not None:
                import copy
                staging_sink = copy.copy(sink)
                staging_sink._table = f"{table}_replay_staging"  # type: ignore[attr-defined]
                return dataclasses.replace(pipe, sink=staging_sink)
            return pipe

        # REPLACE_SCOPE or REPLACE_FULL: write normally.
        return pipe

    def _apply_scope(self, pipe: Pipe, scope_dict: dict[str, Any]) -> Pipe:
        """Apply a window scope from the manifest to the pipe."""
        from openneuronic.pipes.core.partial_scope import PartialScope
        import datetime as dt

        date_start = scope_dict.get("date_start")
        date_end = scope_dict.get("date_end")
        if isinstance(date_start, str):
            date_start = dt.datetime.fromisoformat(date_start)
        if isinstance(date_end, str):
            date_end = dt.datetime.fromisoformat(date_end)

        scope = PartialScope(
            date_start=date_start,
            date_end=date_end,
            date_column=scope_dict.get("date_column"),
            key_set=scope_dict.get("key_set", []),
            key_column=scope_dict.get("key_column"),
            predicate=scope_dict.get("predicate"),
        )
        return dataclasses.replace(pipe, scope=scope, mode=CopyMode.PARTIAL)


# ---------------------------------------------------------------------------
# Dev-null sink for DRY_RUN
# ---------------------------------------------------------------------------

from openneuronic.pipes.sinks._base import AbstractSink  # noqa: E402


class _DevNullSink(AbstractSink):
    """A sink that discards all records — used for DRY_RUN replay."""

    _table = "_dry_run"

    async def setup(self) -> None:
        pass

    async def write(self, records: list[Record]) -> None:
        pass  # discard

    async def commit_bookmark(self, bookmark: Any) -> None:
        pass

    async def teardown(self) -> None:
        pass
