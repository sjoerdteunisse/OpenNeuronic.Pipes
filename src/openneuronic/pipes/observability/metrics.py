"""Optional Prometheus metrics export for OpenNeuronic.Pipes.

Requires the ``metrics`` extra::

    pip install 'openneuronic-pipes[metrics]'

When ``prometheus_client`` is not installed the :class:`PipeMetrics` instance
degrades gracefully to no-ops so importing this module never raises.
"""

from __future__ import annotations


class PipeMetrics:
    """Prometheus Counter / Histogram wrappers for all core pipe events.

    Instantiate once at application startup and pass to ``LocalRunner``
    (or ``OpusRunner`` in M6) via the ``metrics_exporter`` parameter.

    All methods are safe to call when ``prometheus_client`` is not installed
    — they become no-ops and :attr:`available` returns ``False``.
    """

    def __init__(self, namespace: str = "onpipes") -> None:
        try:
            import prometheus_client as prom  # noqa: PLC0415

            self._records_read = prom.Counter(
                f"{namespace}_records_read_total",
                "Total records read from source",
                ["pipe_id"],
            )
            self._records_written = prom.Counter(
                f"{namespace}_records_written_total",
                "Total records written to sink",
                ["pipe_id"],
            )
            self._records_dropped = prom.Counter(
                f"{namespace}_records_dropped_total",
                "Total records dropped by processors or guards",
                ["pipe_id"],
            )
            self._errors = prom.Counter(
                f"{namespace}_errors_total",
                "Total errors during pipe execution",
                ["pipe_id"],
            )
            self._batch_duration = prom.Histogram(
                f"{namespace}_batch_duration_seconds",
                "Per-batch processing duration in seconds",
                ["pipe_id"],
            )
            self._queue_depth = prom.Gauge(
                f"{namespace}_queue_depth",
                "Current broker queue depth",
                ["pipe_id"],
            )
            self._lag_seconds = prom.Gauge(
                f"{namespace}_lag_seconds",
                "Source lag in seconds relative to current time",
                ["pipe_id"],
            )
            self._contract_failures = prom.Counter(
                f"{namespace}_contract_failures_total",
                "Total contract enforcement failures",
                ["pipe_id", "stage"],
            )
            self._replay_runs = prom.Counter(
                f"{namespace}_replay_runs_total",
                "Total replay runs executed",
                ["pipe_id"],
            )
            self._durable_resumes = prom.Counter(
                f"{namespace}_durable_resume_total",
                "Total durable run resumes after crash",
                ["opus_id"],
            )
            self._available = True
        except ImportError:
            self._available = False

    @property
    def available(self) -> bool:
        """``True`` when prometheus_client is installed."""
        return self._available

    # ------------------------------------------------------------------
    # Record counters
    # ------------------------------------------------------------------

    def inc_records_read(self, pipe_id: str, count: int = 1) -> None:
        if self._available:
            self._records_read.labels(pipe_id=pipe_id).inc(count)

    def inc_records_written(self, pipe_id: str, count: int = 1) -> None:
        if self._available:
            self._records_written.labels(pipe_id=pipe_id).inc(count)

    def inc_records_dropped(self, pipe_id: str, count: int = 1) -> None:
        if self._available:
            self._records_dropped.labels(pipe_id=pipe_id).inc(count)

    def inc_errors(self, pipe_id: str) -> None:
        if self._available:
            self._errors.labels(pipe_id=pipe_id).inc()

    # ------------------------------------------------------------------
    # Timing / gauges
    # ------------------------------------------------------------------

    def observe_batch_duration(self, pipe_id: str, seconds: float) -> None:
        if self._available:
            self._batch_duration.labels(pipe_id=pipe_id).observe(seconds)

    def set_queue_depth(self, pipe_id: str, depth: int) -> None:
        if self._available:
            self._queue_depth.labels(pipe_id=pipe_id).set(depth)

    def set_lag_seconds(self, pipe_id: str, lag: float) -> None:
        if self._available:
            self._lag_seconds.labels(pipe_id=pipe_id).set(lag)

    # ------------------------------------------------------------------
    # Contract / replay / durability
    # ------------------------------------------------------------------

    def inc_contract_failures(self, pipe_id: str, stage: str) -> None:
        if self._available:
            self._contract_failures.labels(pipe_id=pipe_id, stage=stage).inc()

    def inc_replay_runs(self, pipe_id: str) -> None:
        if self._available:
            self._replay_runs.labels(pipe_id=pipe_id).inc()

    def inc_durable_resumes(self, opus_id: str) -> None:
        if self._available:
            self._durable_resumes.labels(opus_id=opus_id).inc()
