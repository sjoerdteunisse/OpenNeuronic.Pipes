from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes.core.record import Record
from openneuronic.pipes.guards.base import GuardContext, GuardResult
from openneuronic.pipes.guards.deviation import (
    EmptyColumnGuard,
    FreshnessGuard,
    NullSpikeGuard,
)
from openneuronic.pipes.guards.reliability import (
    CircuitBreakerGuard,
    RetryGuard,
    TimeoutGuard,
)
from openneuronic.pipes.guards.suite import GuardSuite, GuardSuiteRef, guard_suite_registry
from openneuronic.pipes.guards.verification import (
    DuplicateCheckGuard,
    RowCountGuard,
    SchemaMatchGuard,
)
from openneuronic.pipes.schema.base import Schema
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType


def _ctx(*payloads: dict, **meta) -> GuardContext:
    records = [Record(payload=p) for p in payloads]
    return GuardContext(pipe_id="test", batch=records, metadata=dict(meta))


# ---------------------------------------------------------------------------
# RetryGuard
# ---------------------------------------------------------------------------


async def test_retry_guard_allows_up_to_max() -> None:
    g = RetryGuard(max_retries=3)
    for _ in range(3):
        r = await g.check(_ctx())
        assert r.passed

    r = await g.check(_ctx())
    assert not r.passed
    assert "exceeded" in r.message


async def test_retry_guard_resets() -> None:
    g = RetryGuard(max_retries=2)
    await g.check(_ctx())
    await g.check(_ctx())
    g.reset()
    r = await g.check(_ctx())
    assert r.passed


# ---------------------------------------------------------------------------
# TimeoutGuard
# ---------------------------------------------------------------------------


async def test_timeout_guard_passes_within_limit() -> None:
    g = TimeoutGuard(timeout_s=5.0)
    r = await g.check(_ctx(duration_s=2.0))
    assert r.passed


async def test_timeout_guard_fails_over_limit() -> None:
    g = TimeoutGuard(timeout_s=1.0)
    r = await g.check(_ctx(duration_s=3.0))
    assert not r.passed
    assert "exceeded" in r.message.lower() or "timeout" in r.message.lower()


async def test_timeout_guard_skips_without_duration() -> None:
    g = TimeoutGuard(timeout_s=1.0)
    r = await g.check(_ctx())
    assert r.passed


# ---------------------------------------------------------------------------
# CircuitBreakerGuard
# ---------------------------------------------------------------------------


async def test_circuit_breaker_stays_closed_under_threshold() -> None:
    g = CircuitBreakerGuard(failure_threshold=3, recovery_s=60)
    for _ in range(2):
        r = await g.check(_ctx(batch_failed=True))
        assert r.passed
    assert not g.is_open


async def test_circuit_breaker_opens_at_threshold() -> None:
    g = CircuitBreakerGuard(failure_threshold=3, recovery_s=60)
    for _ in range(3):
        await g.check(_ctx(batch_failed=True))
    assert g.is_open
    r = await g.check(_ctx())
    assert not r.passed
    assert "OPEN" in r.message


async def test_circuit_breaker_closes_on_success() -> None:
    g = CircuitBreakerGuard(failure_threshold=2, recovery_s=60)
    await g.check(_ctx(batch_failed=True))
    await g.check(_ctx(batch_failed=True))
    assert g.is_open
    # Simulate recovery window elapsed by reaching into internals
    import time
    g._opened_at = time.monotonic() - 61  # type: ignore[attr-defined]
    # Half-open probe — success
    await g.check(_ctx(batch_failed=False))
    assert not g.is_open


# ---------------------------------------------------------------------------
# RowCountGuard
# ---------------------------------------------------------------------------


async def test_row_count_guard_passes_within_bounds() -> None:
    g = RowCountGuard(min_rows=1, max_rows=100)
    r = await g.check(_ctx({"id": 1}, {"id": 2}))
    assert r.passed


async def test_row_count_guard_fails_below_min() -> None:
    g = RowCountGuard(min_rows=5)
    r = await g.check(_ctx({"id": 1}))
    assert not r.passed
    assert "below minimum" in r.message


async def test_row_count_guard_fails_above_max() -> None:
    g = RowCountGuard(max_rows=2)
    r = await g.check(_ctx({"id": 1}, {"id": 2}, {"id": 3}))
    assert not r.passed
    assert "exceeds maximum" in r.message


async def test_row_count_guard_no_bounds_always_passes() -> None:
    g = RowCountGuard()
    r = await g.check(_ctx())
    assert r.passed


# ---------------------------------------------------------------------------
# DuplicateCheckGuard
# ---------------------------------------------------------------------------


async def test_duplicate_check_passes_unique_keys() -> None:
    g = DuplicateCheckGuard(key_fields=["id"])
    r = await g.check(_ctx({"id": 1}, {"id": 2}, {"id": 3}))
    assert r.passed


async def test_duplicate_check_fails_on_duplicates() -> None:
    g = DuplicateCheckGuard(key_fields=["id"])
    r = await g.check(_ctx({"id": 1}, {"id": 1}, {"id": 2}))
    assert not r.passed
    assert "duplicate" in r.message.lower()


async def test_duplicate_check_composite_key() -> None:
    g = DuplicateCheckGuard(key_fields=["tenant_id", "order_id"])
    r = await g.check(_ctx(
        {"tenant_id": 1, "order_id": 1},
        {"tenant_id": 1, "order_id": 2},
        {"tenant_id": 2, "order_id": 1},
    ))
    assert r.passed


# ---------------------------------------------------------------------------
# SchemaMatchGuard
# ---------------------------------------------------------------------------


class _SampleSchema(Schema):
    id   = Field(FieldType.UUID, nullable=False, primary_key=True)
    name = Field(FieldType.STRING, nullable=True)

_SampleSchema.__schema_version__ = 1
_SampleSchema.__schema_previous__ = None


async def test_schema_match_guard_passes_known_fields() -> None:
    g = SchemaMatchGuard(_SampleSchema)
    r = await g.check(_ctx({"id": "abc", "name": "x"}))
    assert r.passed


async def test_schema_match_guard_fails_unknown_fields() -> None:
    g = SchemaMatchGuard(_SampleSchema)
    r = await g.check(_ctx({"id": "abc", "name": "x", "ghost": True}))
    assert not r.passed
    assert "ghost" in r.message


# ---------------------------------------------------------------------------
# NullSpikeGuard
# ---------------------------------------------------------------------------


async def test_null_spike_guard_passes_under_threshold() -> None:
    g = NullSpikeGuard("name", max_null_pct=0.5)
    ctx = _ctx({"name": "a"}, {"name": "b"}, {"name": None})
    r = await g.check(ctx)
    assert r.passed  # 33% < 50%


async def test_null_spike_guard_fails_over_threshold() -> None:
    g = NullSpikeGuard("name", max_null_pct=0.2)
    ctx = _ctx({"name": None}, {"name": None}, {"name": "x"})
    r = await g.check(ctx)
    assert not r.passed
    assert "exceeding threshold" in r.message


# ---------------------------------------------------------------------------
# EmptyColumnGuard
# ---------------------------------------------------------------------------


async def test_empty_column_guard_passes_when_has_values() -> None:
    g = EmptyColumnGuard("name")
    r = await g.check(_ctx({"name": "Alice"}, {"name": None}))
    assert r.passed


async def test_empty_column_guard_fails_all_null() -> None:
    g = EmptyColumnGuard("name")
    r = await g.check(_ctx({"name": None}, {"name": None}))
    assert not r.passed


# ---------------------------------------------------------------------------
# FreshnessGuard
# ---------------------------------------------------------------------------


async def test_freshness_guard_passes_recent_data() -> None:
    g = FreshnessGuard("updated_at", max_age_seconds=3600)
    now = datetime.datetime.now(datetime.UTC)
    r = await g.check(_ctx({"updated_at": now}))
    assert r.passed


async def test_freshness_guard_fails_stale_data() -> None:
    g = FreshnessGuard("updated_at", max_age_seconds=60)
    old = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=2)
    r = await g.check(_ctx({"updated_at": old}))
    assert not r.passed
    assert "exceeding freshness threshold" in r.message


async def test_freshness_guard_skips_empty_batch() -> None:
    g = FreshnessGuard("updated_at", max_age_seconds=60)
    r = await g.check(GuardContext(pipe_id="p"))
    assert r.passed


# ---------------------------------------------------------------------------
# GuardSuite
# ---------------------------------------------------------------------------


async def test_guard_suite_runs_all_guards() -> None:
    suite = GuardSuite("test-suite", guards=[
        RowCountGuard(min_rows=1),
        DuplicateCheckGuard(["id"]),
    ])
    passed, results = await suite.run_all(_ctx({"id": 1}, {"id": 2}))
    assert passed
    assert len(results) == 2


async def test_guard_suite_ref_resolves() -> None:
    suite = GuardSuite("my-suite", guards=[RowCountGuard(min_rows=0)])
    guard_suite_registry.register(suite)
    ref = GuardSuiteRef("my-suite")
    resolved = ref.resolve()
    assert resolved is suite


async def test_guard_suite_ref_missing_raises() -> None:
    ref = GuardSuiteRef("nonexistent-suite")
    with pytest.raises(KeyError, match="nonexistent-suite"):
        ref.resolve()
