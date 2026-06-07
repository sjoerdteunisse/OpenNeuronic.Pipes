"""README example tests — Guards & Suites section."""
from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes import Record, GuardSuiteRef
from openneuronic.pipes.guards.base import GuardContext
from openneuronic.pipes.guards.deviation import EmptyColumnGuard, FreshnessGuard, NullSpikeGuard
from openneuronic.pipes.guards.reliability import CircuitBreakerGuard, RetryGuard, TimeoutGuard
from openneuronic.pipes.guards.suite import GuardSuite, guard_suite_registry
from openneuronic.pipes.guards.verification import DuplicateCheckGuard, RowCountGuard


def _ctx(n: int = 2, include_none_name: bool = False) -> GuardContext:
    batch = []
    for i in range(n):
        name = None if (include_none_name and i == 0) else f"user-{i}"
        batch.append(Record(payload={
            "id": i,
            "name": name,
            "created_at": datetime.datetime.now(datetime.UTC),
        }))
    return GuardContext(pipe_id="orders", batch=batch)


# ---------------------------------------------------------------------------
# RowCountGuard
# ---------------------------------------------------------------------------

async def test_row_count_guard_passes_within_range() -> None:
    result = await RowCountGuard(min_rows=1, max_rows=10).check(_ctx(5))
    assert result.passed


async def test_row_count_guard_fails_below_min() -> None:
    result = await RowCountGuard(min_rows=10, max_rows=100).check(_ctx(2))
    assert not result.passed


async def test_row_count_guard_fails_above_max() -> None:
    result = await RowCountGuard(min_rows=1, max_rows=3).check(_ctx(5))
    assert not result.passed


# ---------------------------------------------------------------------------
# DuplicateCheckGuard
# ---------------------------------------------------------------------------

async def test_duplicate_check_passes_unique_keys() -> None:
    result = await DuplicateCheckGuard(key_fields=["id"]).check(_ctx(5))
    assert result.passed


async def test_duplicate_check_fails_on_duplicates() -> None:
    ctx = GuardContext(pipe_id="p", batch=[
        Record(payload={"id": 1}),
        Record(payload={"id": 1}),
    ])
    result = await DuplicateCheckGuard(key_fields=["id"]).check(ctx)
    assert not result.passed


# ---------------------------------------------------------------------------
# NullSpikeGuard
# ---------------------------------------------------------------------------

async def test_null_spike_guard_passes_below_threshold() -> None:
    result = await NullSpikeGuard(field="name", max_null_pct=0.10).check(_ctx(5))
    assert result.passed


async def test_null_spike_guard_fails_above_threshold() -> None:
    result = await NullSpikeGuard(field="name", max_null_pct=0.0).check(_ctx(5, include_none_name=True))
    assert not result.passed


# ---------------------------------------------------------------------------
# EmptyColumnGuard
# ---------------------------------------------------------------------------

async def test_empty_column_guard_passes_all_present() -> None:
    result = await EmptyColumnGuard(field="name").check(_ctx(3))
    assert result.passed


async def test_empty_column_guard_fails_when_none_present() -> None:
    ctx = GuardContext(pipe_id="p", batch=[Record(payload={"name": None})])
    result = await EmptyColumnGuard(field="name").check(ctx)
    assert not result.passed


# ---------------------------------------------------------------------------
# FreshnessGuard
# ---------------------------------------------------------------------------

async def test_freshness_guard_passes_recent_records() -> None:
    ctx = GuardContext(pipe_id="p", batch=[
        Record(payload={"created_at": datetime.datetime.now(datetime.UTC)}),
    ])
    result = await FreshnessGuard(field="created_at", max_age_seconds=3600).check(ctx)
    assert result.passed


async def test_freshness_guard_fails_stale_records() -> None:
    stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=5)
    ctx = GuardContext(pipe_id="p", batch=[Record(payload={"created_at": stale})])
    result = await FreshnessGuard(field="created_at", max_age_seconds=3600).check(ctx)
    assert not result.passed


# ---------------------------------------------------------------------------
# RetryGuard / TimeoutGuard / CircuitBreakerGuard
# ---------------------------------------------------------------------------

def test_retry_guard_constructs() -> None:
    guard = RetryGuard(max_retries=3)
    assert isinstance(guard, RetryGuard)


def test_timeout_guard_constructs() -> None:
    guard = TimeoutGuard(timeout_s=60.0)
    assert isinstance(guard, TimeoutGuard)


def test_circuit_breaker_guard_constructs() -> None:
    guard = CircuitBreakerGuard(failure_threshold=5, recovery_s=30.0)
    assert isinstance(guard, CircuitBreakerGuard)


# ---------------------------------------------------------------------------
# GuardSuites
# ---------------------------------------------------------------------------

def test_guard_suite_registers_and_resolves() -> None:
    suite = GuardSuite("readme-orders-quality-guards", guards=[
        RowCountGuard(min_rows=1),
        DuplicateCheckGuard(key_fields=["id"]),
        NullSpikeGuard(field="id", max_null_pct=0.0),
    ])
    guard_suite_registry.register(suite)

    ref = GuardSuiteRef("readme-orders-quality-guards")
    resolved = ref.resolve()
    assert resolved is not None
    assert resolved.name == "readme-orders-quality-guards"


async def test_guard_suite_run_all_passes() -> None:
    suite_name = "readme-suite-run-all"
    suite = GuardSuite(suite_name, guards=[RowCountGuard(min_rows=1)])
    guard_suite_registry.register(suite)

    passed, results = await suite.run_all(_ctx(3))
    assert passed
    assert len(results) == 1
