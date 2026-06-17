"""Pipe factory — build :class:`~openneuronic.pipes.core.pipe.Pipe` objects
from a JSON-shaped specification dict.

Connection strings and other sensitive fields may be expressed as a secret
reference ``{"$secret": "key_name"}`` instead of inline plaintext.  The
:class:`~openneuronic.pipes.api.secrets.SecretStore` resolves them at
build-time; the resolved value is passed directly to the source/sink
constructor and never stored in the registry.

Supported source types:  ``sqlserver``, ``postgres``, ``memory``
Supported sink types:     ``sqlserver``, ``postgres``, ``memory``
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from openneuronic.pipes.api.secrets import SecretNotFoundError, SecretStore
from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import CopyMode
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource


class FactoryError(ValueError):
    """Raised when a pipe or source/sink spec is invalid or incomplete."""


# ---------------------------------------------------------------------------
# Lightweight in-process source/sink used for "memory" type
# ---------------------------------------------------------------------------


class _ApiMemorySource(AbstractSource):
    """Source that yields a fixed list of record payloads.

    Useful for smoke-testing a dynamically created pipe without a real database.
    """

    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self._payloads = payloads

    async def setup(self) -> None:
        pass

    def read(self, bookmark: Bookmark | None = None) -> AsyncIterator[Record]:
        payloads = self._payloads

        async def _gen() -> AsyncIterator[Record]:
            for p in payloads:
                yield Record(payload=dict(p))

        return _gen()

    async def teardown(self) -> None:
        pass

    @property
    def dataset_name(self) -> str:
        return "api.memory"


class _ApiMemorySink(AbstractSink):
    """Sink that discards all written records (useful for smoke tests)."""

    async def setup(self) -> None:
        pass

    async def write(self, records: list[Record]) -> None:
        pass

    async def commit_bookmark(self, bookmark: Bookmark) -> None:
        pass

    async def teardown(self) -> None:
        pass

    @property
    def dataset_name(self) -> str:
        return "api.memory"


# ---------------------------------------------------------------------------
# Source factory
# ---------------------------------------------------------------------------


def _build_source(spec: dict[str, Any], secrets: SecretStore) -> AbstractSource:
    source_type = spec.get("type", "").lower().strip()

    if source_type == "sqlserver":
        from openneuronic.pipes.sources.sqlserver import SQLServerSource

        connection = str(secrets.resolve(spec.get("connection", "")))
        query = spec.get("query")
        if not query:
            raise FactoryError("SQLServer source requires 'query'")
        return SQLServerSource(
            connection=connection,
            query=query,
            source_id=spec.get("source_id", "sqlserver"),
        )

    if source_type == "postgres":
        from openneuronic.pipes.sources.postgres import PostgresSource

        connection = str(secrets.resolve(spec.get("connection", "")))
        query = spec.get("query")
        if not query:
            raise FactoryError("Postgres source requires 'query'")
        return PostgresSource(
            connection=connection,
            query=query,
            source_id=spec.get("source_id", "postgres"),
        )

    if source_type == "memory":
        payloads = spec.get("payloads", [])
        if not isinstance(payloads, list):
            raise FactoryError("Memory source 'payloads' must be a list")
        return _ApiMemorySource(payloads=payloads)

    raise FactoryError(
        f"Unknown source type: {source_type!r}. "
        "Supported types: sqlserver, postgres, memory"
    )


# ---------------------------------------------------------------------------
# Sink factory
# ---------------------------------------------------------------------------


def _build_sink(spec: dict[str, Any], secrets: SecretStore) -> AbstractSink:
    sink_type = spec.get("type", "").lower().strip()

    if sink_type == "sqlserver":
        from openneuronic.pipes.sinks.sqlserver import SQLServerSink

        connection = str(secrets.resolve(spec.get("connection", "")))
        target_table = spec.get("target_table")
        if not target_table:
            raise FactoryError("SQLServer sink requires 'target_table'")
        upsert_key = spec.get("upsert_key")
        return SQLServerSink(
            connection=connection,
            target_table=target_table,
            upsert_key=upsert_key,
            auto_migrate=bool(spec.get("auto_migrate", False)),
        )

    if sink_type == "postgres":
        from openneuronic.pipes.sinks.postgres import PostgresSink

        connection = str(secrets.resolve(spec.get("connection", "")))
        target_table = spec.get("target_table")
        if not target_table:
            raise FactoryError("Postgres sink requires 'target_table'")
        upsert_key = spec.get("upsert_key")
        return PostgresSink(
            connection=connection,
            target_table=target_table,
            upsert_key=upsert_key,
            auto_migrate=bool(spec.get("auto_migrate", False)),
        )

    if sink_type == "memory":
        return _ApiMemorySink()

    raise FactoryError(
        f"Unknown sink type: {sink_type!r}. "
        "Supported types: sqlserver, postgres, memory"
    )


# ---------------------------------------------------------------------------
# Pipe factory
# ---------------------------------------------------------------------------


def build_pipe(spec: dict[str, Any], secrets: SecretStore) -> Pipe:
    """Construct a :class:`~openneuronic.pipes.core.pipe.Pipe` from *spec*.

    *spec* shape::

        {
            "id":     "orders-sync",          # required, unique
            "mode":   "incremental",           # full | partial | incremental
            "source": {
                "type":       "sqlserver",     # sqlserver | postgres | memory
                "connection": {"$secret": "src_conn"},  # or plain string
                "query":      "SELECT * FROM orders WHERE updated_at > ?"
            },
            "sink": {
                "type":         "sqlserver",
                "connection":   {"$secret": "dst_conn"},
                "target_table": "orders_dest",
                "upsert_key":   "id",          # optional
                "auto_migrate": false          # optional
            }
        }

    Raises:
        :class:`FactoryError` — invalid or incomplete spec.
        :class:`~openneuronic.pipes.api.secrets.SecretNotFoundError` — a
            ``{"$secret": "key"}`` reference points to a key not in the store.
    """
    pipe_id = spec.get("id", "").strip()
    if not pipe_id:
        raise FactoryError("'id' is required and must be non-empty")

    source_spec = spec.get("source")
    if not isinstance(source_spec, dict):
        raise FactoryError("'source' must be an object with at least a 'type' field")

    sink_spec = spec.get("sink")
    if not isinstance(sink_spec, dict):
        raise FactoryError("'sink' must be an object with at least a 'type' field")

    mode_str = str(spec.get("mode", "incremental")).lower().strip()
    try:
        mode = CopyMode(mode_str)
    except ValueError:
        raise FactoryError(
            f"Invalid mode: {mode_str!r}. Supported: full, partial, incremental"
        )

    source = _build_source(source_spec, secrets)
    sink = _build_sink(sink_spec, secrets)

    return Pipe(id=pipe_id, source=source, sink=sink, mode=mode)
