from __future__ import annotations

import dataclasses
import json
import logging
import sys
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.metrics.base import RunMetrics
    from openneuronic.pipes.runner import RunResult


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        }
        # Merge any extra fields attached to the record.
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            } and not key.startswith("_"):
                data[key] = value
        return json.dumps(data, default=str)


def _make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.propagate = False
    return logger


class PipeLogger:
    """Structured JSON logger scoped to a single pipe run.

    All log lines are emitted to stdout as newline-delimited JSON and include
    the standard fields required by the spec.
    """

    def __init__(self, pipe_id: str, run_id: str = "") -> None:
        self._pipe_id = pipe_id
        self._run_id = run_id
        self._logger = _make_logger(f"openneuronic.pipes.{pipe_id}")
        self._logger.setLevel(logging.DEBUG)

    def _extra(self, **kwargs: Any) -> dict[str, Any]:
        return {"pipe_id": self._pipe_id, "run_id": self._run_id, **kwargs}

    def run_start(
        self,
        source: str = "",
        sink: str = "",
        mode: str = "",
        schema_version: int = 0,
        contract_version: int = 0,
        bookmark: Any = None,
    ) -> None:
        self._logger.info(
            "run_start",
            extra=self._extra(
                status="starting",
                source=source,
                sink=sink,
                mode=mode,
                schema_version=schema_version,
                contract_version=contract_version,
                bookmark=str(bookmark) if bookmark is not None else None,
            ),
        )

    def batch_written(
        self,
        batch_num: int,
        records_in_batch: int,
        records_written: int,
    ) -> None:
        self._logger.debug(
            "batch_written",
            extra=self._extra(
                status="batch_ok",
                batch_num=batch_num,
                records_in_batch=records_in_batch,
                records_written=records_written,
            ),
        )

    def run_end(
        self,
        result: RunResult,
        metrics: RunMetrics | None = None,
    ) -> None:
        status = "success" if result.success else "failed"
        extra = self._extra(
            status=status,
            records_read=result.records_read,
            records_written=result.records_written,
            records_dropped=result.records_dropped,
            duration_ms=result.finished_at and result.started_at and round(
                (result.finished_at - result.started_at).total_seconds() * 1000, 3
            ),
            error_type=type(result.error).__name__ if result.error else None,
        )
        if metrics is not None:
            extra["metrics"] = dataclasses.asdict(metrics)
        self._logger.info("run_end", extra=extra)

    def error(self, message: str, error: Exception | None = None) -> None:
        self._logger.error(
            message,
            extra=self._extra(
                status="error",
                error_type=type(error).__name__ if error else None,
                error_message=str(error) if error else None,
            ),
        )
