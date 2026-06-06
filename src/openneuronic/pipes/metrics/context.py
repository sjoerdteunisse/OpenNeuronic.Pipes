from __future__ import annotations

import time
from dataclasses import dataclass, field

from openneuronic.pipes.metrics.base import RunMetrics


@dataclass
class MetricsContext:
    """Mutable accumulator for metrics during a single pipe run.

    Usage::

        ctx = MetricsContext(pipe_id="orders-sync")
        ctx.start()
        # ... for each batch:
        ctx.record_batch(read=100, written=98, dropped=2)
        # ... on error:
        ctx.record_error()
        ctx.stop()
        metrics = ctx.snapshot()
    """

    pipe_id: str
    records_read: int = field(default=0, init=False)
    records_written: int = field(default=0, init=False)
    records_dropped: int = field(default=0, init=False)
    batches: int = field(default=0, init=False)
    errors: int = field(default=0, init=False)
    _started_at: float | None = field(default=None, init=False, repr=False)
    _stopped_at: float | None = field(default=None, init=False, repr=False)
    # Accumulated quality data for measure implementations.
    _null_counts: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _total_for_null: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _duplicate_count: int = field(default=0, init=False, repr=False)
    _row_count_expected: int | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        self._started_at = time.monotonic()

    def stop(self) -> None:
        self._stopped_at = time.monotonic()

    def record_batch(self, read: int, written: int, dropped: int) -> None:
        self.records_read += read
        self.records_written += written
        self.records_dropped += dropped
        self.batches += 1

    def record_error(self) -> None:
        self.errors += 1

    def record_null(self, field_name: str, null_count: int, total: int) -> None:
        self._null_counts[field_name] = self._null_counts.get(field_name, 0) + null_count
        self._total_for_null[field_name] = self._total_for_null.get(field_name, 0) + total

    def record_duplicates(self, count: int) -> None:
        self._duplicate_count += count

    def set_expected_row_count(self, expected: int) -> None:
        self._row_count_expected = expected

    @property
    def duration_ms(self) -> float:
        if self._started_at is None:
            return 0.0
        end = self._stopped_at if self._stopped_at is not None else time.monotonic()
        return (end - self._started_at) * 1000.0

    @property
    def throughput_rps(self) -> float:
        duration_s = self.duration_ms / 1000.0
        if duration_s <= 0:
            return 0.0
        return self.records_written / duration_s

    def snapshot(self) -> RunMetrics:
        null_rates: dict[str, float] = {
            fname: (
                self._null_counts[fname] / self._total_for_null[fname]
                if self._total_for_null.get(fname, 0) > 0
                else 0.0
            )
            for fname in self._null_counts
        }

        duplicate_rate: float | None = None
        if self.records_read > 0 and self._duplicate_count > 0:
            duplicate_rate = self._duplicate_count / self.records_read

        row_count_deviation: float | None = None
        if self._row_count_expected and self._row_count_expected > 0:
            row_count_deviation = (
                abs(self.records_written - self._row_count_expected)
                / self._row_count_expected
            )

        return RunMetrics(
            pipe_id=self.pipe_id,
            records_read=self.records_read,
            records_written=self.records_written,
            records_dropped=self.records_dropped,
            batches=self.batches,
            errors=self.errors,
            duration_ms=round(self.duration_ms, 3),
            throughput_rps=round(self.throughput_rps, 2),
            null_rates=null_rates,
            duplicate_rate=duplicate_rate,
            row_count_deviation=row_count_deviation,
        )
