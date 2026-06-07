# OpenNeuronic.Pipes

A Python-first, horizontally scalable, containerised data movement and transformation platform. Move data between SQL Server, PostgreSQL, and other systems with explicit schema contracts, durable orchestration, replayability, and end-to-end lineage.

**Distribution name:** `openneuronic-pipes` · **Import path:** `openneuronic.pipes`

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Records](#records)
4. [Schema System](#schema-system)
5. [Contracts](#contracts)
6. [Copy Modes](#copy-modes)
7. [Bookmarks](#bookmarks)
8. [Guards & Suites](#guards--suites)
9. [Measures & Metrics](#measures--metrics)
10. [Lineage & Knowledge Graph](#lineage--knowledge-graph)
11. [Replay & Time Travel](#replay--time-travel)
12. [Broker](#broker)
13. [Opus Orchestration](#opus-orchestration)
14. [Deployment](#deployment)
15. [CLI Reference](#cli-reference)

---

## Installation

```bash
pip install openneuronic-pipes             # core only
pip install "openneuronic-pipes[postgres]" # + asyncpg
pip install "openneuronic-pipes[sqlserver]"# + aioodbc
pip install "openneuronic-pipes[redis]"    # + redis[asyncio]
pip install "openneuronic-pipes[deploy]"   # + pyyaml
pip install "openneuronic-pipes[dev]"      # all dev extras (typer, pyyaml, pytest…)
pip install "openneuronic-pipes[all]"      # everything
```

---

## Quick Start

```python
import asyncio
from openneuronic.pipes import Pipe, CopyMode, LocalRunner
from openneuronic.pipes.sources.sqlserver import SQLServerSource
from openneuronic.pipes.sinks.sqlserver import SQLServerSink

source = SQLServerSource(
    connection="DRIVER={ODBC Driver 17 for SQL Server};Server=tcp:127.0.0.1,1433;Database=source_db;UID=sa;PWD=pass;TrustServerCertificate=yes",
    query="SELECT id, name FROM dbo.Customers",
)
sink = SQLServerSink(
    connection="DRIVER={ODBC Driver 17 for SQL Server};Server=tcp:127.0.0.1,1433;Database=dest_db;UID=sa;PWD=pass;TrustServerCertificate=yes",
    target_table="Customers",
    upsert_key="id",
)

pipe = Pipe(id="customers-sync", source=source, sink=sink, mode=CopyMode.INCREMENTAL)

async def main():
    result = await LocalRunner().run(pipe)
    print(f"Copied {result.records_written} records  success={result.success}")

asyncio.run(main())
```

---

## Records

A `Record` is the canonical unit of data in flight. It carries a `payload` dict, operational `metadata`, and version/provenance fields.

```python
from openneuronic.pipes import Record
import datetime

# Records get a unique UUID id by default
r1 = Record(payload={"id": 1, "name": "Alice"})
r2 = Record(payload={"id": 2, "name": "Bob"})
assert r1.id != r2.id                   # unique per instance

# Version and provenance fields
r = Record(
    payload={"order_id": "abc"},
    metadata={"trace_id": "xyz-123"},
    schema_version=2,
    contract_version=1,
)
assert r.schema_version == 2
assert r.metadata["trace_id"] == "xyz-123"
assert r.emitted_at.tzinfo is not None  # always UTC-aware
```

**Fields:**

| Field | Type | Default | Description |
|---|---|---|---|
| `id` | `str` | UUID4 | Unique record identifier (for deduplication) |
| `payload` | `dict` | `{}` | Abstract schema-shaped data |
| `metadata` | `dict` | `{}` | Lineage, trace IDs, replay references |
| `source_id` | `str` | `""` | Originating source identifier |
| `pipe_id` | `str` | `""` | Set by the runner at execution time |
| `schema_version` | `int` | `1` | Schema version the record was emitted under |
| `contract_version` | `int` | `1` | Contract version the record was validated under |
| `emitted_at` | `datetime` | UTC now | UTC-aware timestamp |

---

## Schema System

Schemas are defined once in abstract form using `Field` and `FieldType`. Concrete DDL for each database is generated from the same definition.

### Defining a schema

```python
from openneuronic.pipes.schema.base import Schema, schema_version
from openneuronic.pipes.schema.field import Field
from openneuronic.pipes.schema.field_type import FieldType

@schema_version(1)
class OrderSchemaV1(Schema):
    id          = Field(FieldType.UUID,        nullable=False, primary_key=True)
    tenant_id   = Field(FieldType.STRING,      max_length=100, nullable=True)
    total       = Field(FieldType.DECIMAL,     precision=18, scale=4)
    created_at  = Field(FieldType.DATETIME_TZ, nullable=False)
    status      = Field(FieldType.STRING,      max_length=20, nullable=False, default="pending")
```

### Field types

`INTEGER` · `BIGINT` · `SMALLINT` · `DECIMAL` · `FLOAT` · `BOOLEAN` · `STRING` · `TEXT` · `CHAR` · `DATE` · `TIME` · `DATETIME` · `DATETIME_TZ` · `DURATION` · `UUID` · `JSON` · `BYTES` · `ARRAY` · `ROWVERSION` · `SERIAL`

### Validation

```python
errors = OrderSchemaV1().validate({"id": None, "created_at": "2024-01-01"})
# ["Field 'id' is non-nullable but value is None"]
```

### Schema evolution

New versions extend the previous version — never edit:

```python
@schema_version(2, previous=OrderSchemaV1)
class OrderSchemaV2(OrderSchemaV1):
    currency = Field(FieldType.STRING, max_length=3, nullable=True)
```

### Diffing schemas

```python
from openneuronic.pipes.schema.diff import diff_schemas, ChangeKind

diff = diff_schemas(OrderSchemaV1, OrderSchemaV2)
for change in diff.changes:
    print(f"{change.name}: {change.kind}")  # currency: safe
assert not diff.has_breaking_changes
```

### Runtime migration

Records from an older producer are upgraded in-flight before reaching processors or the sink:

```python
from openneuronic.pipes.schema.migration import Migration, MigrationStep, RuntimeUpgrader, migration_registry

migration = Migration(
    from_version=1,
    to_version=2,
    steps=[MigrationStep.fill("currency", "EUR")],
    ddl_up="ALTER TABLE orders ADD currency NVARCHAR(3) NULL",
)

# Register on the global migration registry
migration_registry.register("OrderSchemaV1", migration)

# Upgrade a payload dict from v1 to v2
upgrader = RuntimeUpgrader()
old_payload = {"id": "abc", "total": "100.00"}
new_payload = upgrader.upgrade(old_payload, schema_name="OrderSchemaV1", from_version=1, to_version=2)
assert new_payload["currency"] == "EUR"
```

### DDL generation

```python
from openneuronic.pipes.schema.mapper.postgres import create_table_ddl as pg_ddl
from openneuronic.pipes.schema.mapper.sqlserver import create_table_ddl as ss_ddl

print(pg_ddl("orders", OrderSchemaV1))
# CREATE TABLE IF NOT EXISTS "orders" ("id" UUID NOT NULL, ...)

print(ss_ddl("orders", OrderSchemaV1))
# IF OBJECT_ID('[dbo].[orders]', 'U') IS NULL CREATE TABLE [orders] ([id] UNIQUEIDENTIFIER NOT NULL, ...)
```

### Auto-migrate at runtime

```python
sink = SQLServerSink(connection="...", target_table="orders", auto_migrate=True)
pipe = Pipe(id="orders", source=source, sink=sink, mode=CopyMode.FULL, schema=OrderSchemaV1)
# LocalRunner will call sink.apply_schema(OrderSchemaV1) before writing
```

---

## Contracts

A contract is a named, versioned set of guarantees a producer makes to consumers beyond what the schema expresses.

### Defining a contract

```python
from openneuronic.pipes.contracts.base import Contract, contract_version
from openneuronic.pipes.core.enums import CompatibilityMode

@contract_version(1)
class OrderContract(Contract):
    schema                      = OrderSchemaV1
    primary_key                 = ["id"]
    required_fields             = ["id", "created_at", "status"]
    compatibility               = CompatibilityMode.FORWARD
    owner                       = "data-platform"
    freshness_sla               = "1h"
    max_row_count_deviation_pct = 0.10
    required_guard_suites       = ["orders-quality"]
    required_measure_sets       = ["core-runtime"]
    accepted_schema_versions    = [1, 2]
```

### Contract enforcement

Enforcement runs at four lifecycle points:

```python
from openneuronic.pipes.contracts.enforcement import contract_enforcer
from openneuronic.pipes import Record
import datetime

# 1. At deploy time — static configuration checks
result = contract_enforcer.enforce_deploy(OrderContract)
assert result.passed

# 2. At run start — schema version compatibility
result = contract_enforcer.enforce_run_start(OrderContract, incoming_schema_version=1)
assert result.passed

# 3. Before load — record-level validation
records = [Record(payload={"id": "abc", "created_at": "2024-01-01", "status": "active"})]
result = contract_enforcer.enforce_pre_load(OrderContract, records)
assert result.passed

# 4. At publish — quality and freshness guarantees
now  = datetime.datetime.now(datetime.UTC)
last = now - datetime.timedelta(minutes=30)
result = contract_enforcer.enforce_publish(
    OrderContract,
    records_written=1000,
    records_expected=1010,
    run_finished_at=now,
    last_successful_run_at=last,
)
assert result.passed  # 1% deviation < 10%, 30m gap < 1h SLA
```

### Freshness SLA format

`"30m"` (30 minutes) · `"1h"` (1 hour) · `"2d"` (2 days) · `"90"` (90 seconds)

### Compatibility modes

| Mode | Meaning |
|---|---|
| `STRICT` | Source and sink schema version must match exactly |
| `FORWARD` | Old records can be upgraded to the current version |
| `BACKWARD` | New records can be downgraded for legacy sinks |
| `FULL` | Mixed-version interoperability in both directions |

### Registry

```python
from openneuronic.pipes.contracts.registry import contract_registry

all_versions = contract_registry.all_versions("OrderContract")  # [1, 2, ...]
latest       = contract_registry.get_latest("OrderContract")
specific     = contract_registry.get("OrderContract", version=1)
```

---

## Copy Modes

Three modes control how data is moved and how the sink handles existing data.

### INCREMENTAL — bookmark-driven

Only records newer than the last bookmark are read. The bookmark advances only after a successful sink write.

```python
from openneuronic.pipes import Pipe, CopyMode, Bookmark, BookmarkType
import datetime

bookmark = Bookmark(
    pipe_id="orders-sync",
    column="updated_at",
    type=BookmarkType.DATETIME,
    value=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
)

pipe = Pipe(id="orders-sync", source=source, sink=sink, mode=CopyMode.INCREMENTAL)
result = await LocalRunner().run(pipe, bookmark=bookmark)
# bookmark.value advances to max(updated_at) of written records
```

### FULL — atomic staging swap

All records are written to a staging table first. After all batches succeed, the staging table is atomically swapped with the live table. If the run fails mid-way, the live table is untouched.

```python
pipe = Pipe(id="ref-data", source=source, sink=sink, mode=CopyMode.FULL)
result = await LocalRunner().run(pipe)
# Runner calls: sink.prepare_full_load() → write batches → sink.commit_full_load()
```

### PARTIAL — scoped replacement

Only the records matching the defined scope are replaced. The sink first deletes existing rows in the scope, then writes the new data.

```python
from openneuronic.pipes import PartialScope
import datetime

scope = PartialScope(
    date_column="created_at",
    date_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
    date_end=datetime.datetime(2024, 1, 31, tzinfo=datetime.UTC),
)

pipe = Pipe(id="orders-backfill", source=source, sink=sink, mode=CopyMode.PARTIAL, scope=scope)
result = await LocalRunner().run(pipe)

# Build the WHERE clause manually
where = scope.to_where_clause()
# "created_at >= '2024-01-01T00:00:00+00:00' AND created_at <= '2024-01-31T00:00:00+00:00'"
```

`PartialScope` supports date windows, key sets, and raw predicates:

```python
PartialScope(key_column="tenant_id", key_set=["acme", "globex"])
PartialScope(predicate="status = 'failed' AND retry_count > 3")
```

---

## Bookmarks

Bookmarks track the high-water mark for incremental reads and are committed only after the sink write succeeds.

```python
from openneuronic.pipes import Bookmark, BookmarkType, InMemoryBookmarkStore
import datetime

# Bookmark types
bm_dt  = Bookmark(pipe_id="p", column="updated_at",  type=BookmarkType.DATETIME,   value=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC))
bm_int = Bookmark(pipe_id="p", column="sequence_id", type=BookmarkType.INTEGER,    value=1000)
bm_rv  = Bookmark(pipe_id="p", column="rowversion",  type=BookmarkType.ROWVERSION, value=b"\x00\x00\x00\x00\x00\x00\x00\x01")
bm_lsn = Bookmark(pipe_id="p", column="lsn",         type=BookmarkType.LSN,        value="0/1AF0000")
bm_cur = Bookmark(pipe_id="p", column="cursor",      type=BookmarkType.CURSOR,     value="opaque-string")

# In-memory store
store = InMemoryBookmarkStore()
await store.save(bm_int)

loaded = await store.load("p")
assert loaded.value == 1000

await store.delete("p")
assert await store.load("p") is None

# Redis store (production)
from openneuronic.pipes import RedisBookmarkStore
redis_store = RedisBookmarkStore(redis_url="redis://localhost:6379/0")
```

Rollback safety — bookmark.previous_value holds the last committed position:

```python
bookmark = Bookmark(pipe_id="p", column="id", type=BookmarkType.INTEGER, value=5000, previous_value=4500)
assert bookmark.previous_value == 4500
```

---

## Guards & Suites

Guards are reusable runtime protections that run against a batch and return a `GuardResult`.

### Guard families

```python
from openneuronic.pipes.guards.reliability import RetryGuard, TimeoutGuard, CircuitBreakerGuard
from openneuronic.pipes.guards.verification import RowCountGuard, DuplicateCheckGuard, SchemaMatchGuard
from openneuronic.pipes.guards.deviation import NullSpikeGuard, EmptyColumnGuard, FreshnessGuard
from openneuronic.pipes.guards.base import GuardContext
from openneuronic.pipes import Record
import datetime

ctx = GuardContext(pipe_id="orders", batch=[
    Record(payload={"id": 1, "name": "Alice", "created_at": datetime.datetime.now(datetime.UTC)}),
    Record(payload={"id": 2, "name": "Bob",   "created_at": datetime.datetime.now(datetime.UTC)}),
])

row_count  = RowCountGuard(min_rows=1, max_rows=10_000)
no_dups    = DuplicateCheckGuard(key_fields=["id"])
null_spike = NullSpikeGuard(field="name", max_null_pct=0.05)
not_empty  = EmptyColumnGuard(field="name")
fresh      = FreshnessGuard(field="created_at", max_age_seconds=3600)

result = await row_count.check(ctx)
assert result.passed
```

### Guard suites

```python
from openneuronic.pipes.guards.suite import GuardSuite, guard_suite_registry, GuardSuiteRef

orders_suite = GuardSuite("orders-quality", guards=[
    RowCountGuard(min_rows=1),
    DuplicateCheckGuard(key_fields=["id"]),
    NullSpikeGuard(field="id", max_null_pct=0.0),
])
guard_suite_registry.register(orders_suite)

# Attach to a pipe by reference
pipe = Pipe(
    id="orders-sync", source=source, sink=sink, mode=CopyMode.INCREMENTAL,
    guards=[GuardSuiteRef("orders-quality")],
)

suite = GuardSuiteRef("orders-quality").resolve()
passed, results = await suite.run_all(ctx)
assert passed
```

### Contract guard suite enforcement

When a contract declares `required_guard_suites`, `enforce_deploy` verifies the suite is registered:

```python
result = contract_enforcer.enforce_deploy(OrderContract)
# Fails with violation if "orders-quality" is not in guard_suite_registry
```

---

## Measures & Metrics

Measures are opt-in telemetry policies. Metrics are emitted only when `pipe.measures` is non-empty.

### Defining a measure set

```python
from openneuronic.pipes.metrics.base import MeasureSet, MeasureSetRef, measure_set_registry
from openneuronic.pipes.metrics.measures.runtime import RuntimeMeasure
from openneuronic.pipes.metrics.measures.quality import NullRateMeasure, DuplicateRateMeasure

core_set = MeasureSet("core-runtime", measures=[
    RuntimeMeasure(),
    NullRateMeasure(),          # reports null rates per field from MetricsContext
    DuplicateRateMeasure(),     # reports duplicate record rate from MetricsContext
])
measure_set_registry.register(core_set)

pipe = Pipe(
    id="orders-sync", source=source, sink=sink, mode=CopyMode.INCREMENTAL,
    measures=[MeasureSetRef("core-runtime")],
)
```

### RunMetrics

```python
result = await LocalRunner().run(pipe)
m = result.metrics
if m:
    print(m.records_read, m.records_written, m.throughput_rps, m.duration_ms)
    print(m.null_rates)  # {"name": 0.02, "email": 0.05}
```

### MetricsContext (manual)

```python
from openneuronic.pipes.metrics.context import MetricsContext

ctx = MetricsContext(pipe_id="orders")
ctx.start()
ctx.record_batch(read=500, written=498, dropped=2)
ctx.record_null(field_name="email", null_count=10, total=500)
ctx.stop()

m = ctx.snapshot()
assert m.records_read == 500
assert m.null_rates["email"] == pytest.approx(0.02, abs=1e-9)
```

### @measure decorator

```python
from openneuronic.pipes.metrics.decorators import measure

@measure("my_operation")
async def process_batch(records):
    return [r for r in records if r.payload.get("active")]
```

---

## Lineage & Knowledge Graph

Every run automatically captures dataset-level lineage. Column-level lineage can be declared explicitly.

### Dataset lineage (automatic)

`LocalRunner` emits `READ` and `WRITE` `LineageEvent` objects and attaches them to `RunResult.lineage`.

```python
result = await LocalRunner().run(pipe)
for event in result.lineage:
    print(event.kind, event.pipe_id, event.source_ref, event.target_ref)
```

### Declaring column lineage

```python
from openneuronic.pipes import ColumnLineage, ColumnRef, DatasetRef

source_ds = DatasetRef(name="sqlserver.orders_raw",   schema_name="OrderSchemaV1", schema_version=1)
target_ds = DatasetRef(name="postgres.orders_clean",  schema_name="OrderSchemaV2", schema_version=2)

lineage = ColumnLineage(
    output=ColumnRef(dataset=target_ds, column_name="total_amount_eur"),
    inputs=[
        ColumnRef(dataset=source_ds, column_name="amount"),
        ColumnRef(dataset=source_ds, column_name="currency"),
    ],
    expression="amount * eur_rate",
    transform_kind="derive",
)
```

### LineageTracker

```python
from openneuronic.pipes import LineageTracker, DatasetRef

tracker = LineageTracker()
tracker.track_read("orders-sync",  DatasetRef("sqlserver.orders_raw"),   schema_version=1)
tracker.track_write("orders-sync", DatasetRef("postgres.orders_clean"),  schema_version=2, contract_version=1)

graph = tracker.to_graph()
print(len(list(graph.nodes)))  # 3 (pipe + 2 datasets)
print(len(graph.edges))        # 3 (EXECUTED_IN + READS_FROM + WRITES_TO)
```

### Knowledge graph

```python
from openneuronic.pipes import KnowledgeGraph, NodeKind, EdgeKind

g = KnowledgeGraph()
g.ensure_node("pipe:orders-sync",     NodeKind.PIPE)
g.ensure_node("dataset:orders_raw",   NodeKind.DATASET)
g.ensure_node("dataset:orders_clean", NodeKind.DATASET)
g.add_edge(EdgeKind.READS_FROM, "pipe:orders-sync", "dataset:orders_raw")
g.add_edge(EdgeKind.WRITES_TO,  "pipe:orders-sync", "dataset:orders_clean")

reads = g.edges_of_kind(EdgeKind.READS_FROM)
assert len(reads) == 1
```

### Persisting the graph

```python
from openneuronic.pipes import JsonFileGraphStore

store = JsonFileGraphStore("/var/data/lineage_graph.json")
await store.save(g)

loaded = await store.load()
assert loaded.get_node("pipe:orders-sync") is not None

tracker2 = LineageTracker()
tracker2.track_read("orders-v2", DatasetRef("sqlserver.orders_v2"))
await store.merge_event(tracker2.to_graph())
```

---

## Replay & Time Travel

Every successful run produces a `ReplayPoint` — an immutable snapshot of bookmarks, schema versions, and configuration.

### Automatic replay point capture

```python
result = await LocalRunner().run(pipe)
point = result.replay_point
print(point.replay_id)
print(point.config_digest)
print(point.source_bookmarks)
print(point.schema_versions)
```

### Storing and loading replay points

```python
from openneuronic.pipes import InMemoryReplayStore

store = InMemoryReplayStore()
await store.save(point)

loaded  = await store.load(point.replay_id)
history = await store.list_for_pipe("orders-sync")  # newest-first
```

### Replaying a run

```python
from openneuronic.pipes import ReplayRunner, ReplayManifest, TimeTravelMode, ReplayWriteStrategy

manifest = ReplayManifest(
    replay_point_id=point.replay_id,
    mode=TimeTravelMode.BOOKMARK,
    scope={},
    source_snapshot={},
    sink_strategy=ReplayWriteStrategy.DRY_RUN,
    reason="debug Jan 2024 discrepancy",
)

runner = ReplayRunner(replay_store=store, allow_config_drift=True)
result = await runner.run(pipe, manifest)
print(result.records_written)   # 0 — dry run
```

### Write strategies

| Strategy | Effect |
|---|---|
| `DRY_RUN` | Runs full pipeline but discards all output |
| `SHADOW_WRITE` | Writes to `{table}_shadow` |
| `STAGING_COMPARE` | Writes to `{table}_replay_staging` for manual comparison |
| `REPLACE_SCOPE` | Writes normally with scoped replacement |
| `REPLACE_FULL` | Writes normally with full replacement |

### Window replay

```python
manifest = ReplayManifest(
    replay_point_id=point.replay_id,
    mode=TimeTravelMode.WINDOW,
    scope={
        "date_column": "created_at",
        "date_start":  "2024-01-01T00:00:00+00:00",
        "date_end":    "2024-01-31T23:59:59+00:00",
    },
    source_snapshot={},
    sink_strategy=ReplayWriteStrategy.REPLACE_SCOPE,
)
result = await ReplayRunner(store, allow_config_drift=True).run(pipe, manifest)
# DRY_RUN routes output to an internal null sink — no real table is touched.
assert result.success

The broker layer decouples source readers from processor workers and sink writers for horizontal scaling.

### In-memory broker

```python
from openneuronic.pipes import InMemoryBroker
from openneuronic.pipes.broker.runner import BrokerPipeRunner

broker = InMemoryBroker()
runner = BrokerPipeRunner(broker=broker, batch_size=500)

result = await runner.run(pipe)
print(result.records_published)
print(result.records_processed)
print(result.records_written)
assert result.success
```

### Three-stage model

Each stage runs independently in a separate container:

```python
# Stage 1 — Source reader
published = await runner.read_to_broker(pipe, topic="orders.raw")

# Stage 2 — Processor worker
processed = await runner.process_from_broker(
    pipe, in_topic="orders.raw", out_topic="orders.processed"
)

# Stage 3 — Sink writer
written = await runner.write_from_broker(pipe, topic="orders.processed")
```

### DLQ

```python
dlq = broker.dlq_records("orders.processed")
for record, reason in dlq:
    print(f"DLQ record {record.id}: {reason}")
```

### Production brokers

```python
from openneuronic.pipes.broker.rabbitmq import RabbitMQBroker
from openneuronic.pipes.broker.kafka import KafkaBroker

broker = RabbitMQBroker(url="amqp://guest:guest@localhost/", prefetch=50)
broker = KafkaBroker(bootstrap_servers="localhost:9092", group_id="onpipes-workers")
```

---

## Opus Orchestration

An `Opus` is a composed, durable execution of one or more `PipeSegment` instances with explicit dependency ordering.

### Basic orchestration

```python
from openneuronic.pipes import Opus, PipeSegment, OpusRunner, CopyMode

opus = Opus(id="nightly-etl", durable=True)
opus.add_segments([
    PipeSegment(id="extract",   pipe=extract_pipe),
    PipeSegment(id="transform", pipe=transform_pipe, depends_on=["extract"]),
    PipeSegment(id="load",      pipe=load_pipe,      depends_on=["transform"]),
])

result = await OpusRunner().run(opus)
print(result.success)
print(result.segment_results["load"].run_result.records_written)
```

### Wait strategies

```python
from openneuronic.pipes import WaitStrategy

# ANY — start as soon as one upstream completes
segment = PipeSegment(id="publish", pipe=publish_pipe, depends_on=["reports", "alerts"],
                      wait_strategy=WaitStrategy.ANY)

# MAJORITY — start when ⌈n/2⌉+1 upstreams complete
segment = PipeSegment(id="publish", pipe=publish_pipe, depends_on=["a", "b", "c"],
                      wait_strategy=WaitStrategy.MAJORITY)
```

| Strategy | Meaning |
|---|---|
| `ALL` | Wait for every upstream dependency (default) |
| `ANY` | Start as soon as any upstream completes |
| `MAJORITY` | Start when ⌈n/2⌉+1 upstreams complete |
| `NONE` | Start immediately regardless of dependencies |

### Dynamic fan-out

```python
tenants = ["acme", "globex", "initech"]

opus = Opus(id="tenant-etl")
opus.add_segments([PipeSegment(id="root", pipe=root_pipe)])
opus.add_dynamic_segments(
    segment_factory=lambda tenant: PipeSegment(id=f"load-{tenant}", pipe=build_tenant_pipe(tenant)),
    keys=tenants,
    depends_on=["root"],
    id_prefix="tenant",
)
# Resulting segment IDs: "root", "tenant:acme", "tenant:globex", "tenant:initech"
# (id_prefix is prepended as "{id_prefix}:{key}" to ensure uniqueness)
```

### Retry and failure policies

```python
from openneuronic.pipes import RetryPolicy, GraphFailureMode

segment = PipeSegment(
    id="flaky-api",
    pipe=api_pipe,
    retry_policy=RetryPolicy(max_retries=5, backoff_s=2.0, jitter=True,
                             failure_mode=GraphFailureMode.RETRY_SEGMENT),
)

opus = Opus(id="tolerant-etl", on_failure=GraphFailureMode.CONTINUE_INDEPENDENT_BRANCHES)
```

| Policy | Effect |
|---|---|
| `FAIL_FAST` | Abort the entire opus on first segment failure (default) |
| `RETRY_SEGMENT` | Retry only the failed segment |
| `RETRY_BRANCH` | Retry the failed segment and all downstream dependents |
| `RETRY_OPUS` | Restart the entire opus from scratch |
| `CONTINUE_INDEPENDENT_BRANCHES` | Skip the failed branch; continue unrelated branches |
| `COMPENSATE_AND_STOP` | Run compensation markers, then stop |

### Durable resume

```python
from openneuronic.pipes import InMemoryDurableRunState

store = InMemoryDurableRunState(opus_id="nightly-etl", run_id="run-001")
result = await OpusRunner().run(opus, state_store=store)
# On crash: same store → already-SUCCESS segments are skipped
```

---

## Deployment

### Kubernetes manifests

```python
from openneuronic.pipes.deploy.manifests import ManifestGenerator
from openneuronic.pipes.core.enums import ResourceProfile

gen = ManifestGenerator(
    namespace="production",
    image_prefix="registry.openneuronic.com/onpipes",
    image_tag="v1.2.0",
)

manifests = gen.for_pipe(pipe, profile=ResourceProfile.STANDARD, extra_env={"LOG_LEVEL": "INFO"})
manifests = gen.for_pipe(pipe, schedule="0 2 * * *")          # CronJob
manifests = gen.for_opus(opus, profile=ResourceProfile.HEAVY)

yaml_stream = gen.to_yaml_stream(manifests)
```

### Low-level K8s building blocks

```python
from openneuronic.pipes.deploy.k8s import K8sManifestBuilder, RESOURCE_LIMITS
from openneuronic.pipes.core.enums import ResourceProfile

builder = K8sManifestBuilder(namespace="staging")
deploy  = builder.deployment("orders-runner", "registry/onpipes:latest", {"ENV": "staging"})
cm      = builder.configmap("orders-config", {"pipe_id": "orders-sync"})
sm      = builder.service_monitor("orders-runner")
keda    = builder.keda_scaled_object("orders-keda", "orders-processor", queue_name="orders.processed")
cj      = builder.cronjob("orders-nightly", "0 2 * * *", "registry/onpipes:latest", {})

limits  = RESOURCE_LIMITS[ResourceProfile.HEAVY]
print(limits["cpu_limit"])     # "4000m"
print(limits["memory_limit"])  # "8Gi"
```

### Docker Compose

```python
from openneuronic.pipes.deploy.docker import DockerComposeGenerator

gen     = DockerComposeGenerator()
compose = gen.generate(
    include_sqlserver=True, include_postgres=True,
    include_redis=True, include_rabbitmq=True, include_runner=True,
)
yaml_text = gen.to_yaml(compose)
```

---

## CLI Reference

```bash
pip install "openneuronic-pipes[dev]"
```

```bash
# Version
onpipes version

# Run
onpipes run pipe orders-sync --module myapp.pipes
onpipes run opus nightly-etl --module myapp.opus

# Deploy
onpipes deploy pipe orders-sync --env production --dry-run
onpipes deploy opus nightly-etl --env production --profile heavy --dry-run --output yaml

# Status
onpipes status pipe orders-sync
onpipes status opus nightly-etl

# Schema
onpipes schema diff OrderSchemaV1 1 2
onpipes schema migrate OrderSchemaV1 --to 3 --module myapp.schemas

# Contract
onpipes contract show OrderContract
onpipes contract validate --pipe orders-sync

# Bookmark
onpipes bookmark show orders-sync
onpipes bookmark reset orders-sync

# Replay
onpipes replay create --pipe orders-sync --mode bookmark
onpipes replay run <replay-id> --strategy dry-run

# Lineage
onpipes lineage show --pipe orders-sync
```

---

## Further Reading

- [Integration Tests](tests/integration/) — end-to-end SQL Server examples
- [Example Tests](tests/readme_examples/) — unit tests for every code example in this README

---

## OpenNeuronic commitment to open-source

Open source is not just about making code public; it is about making progress collaborative, transparent, and accessible to anyone willing to contribute.
