from __future__ import annotations

import pytest

from openneuronic.pipes.core.enums import CopyMode, ResourceProfile
from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.core.record import Record
from openneuronic.pipes.deploy.docker import DockerComposeGenerator
from openneuronic.pipes.deploy.k8s import K8sManifestBuilder, RESOURCE_LIMITS
from openneuronic.pipes.deploy.manifests import ManifestGenerator
from openneuronic.pipes.opus.opus import Opus
from openneuronic.pipes.opus.segment import PipeSegment
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource
from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Src(AbstractSource):
    source_id = "stub"
    async def setup(self): pass
    def read(self, bookmark=None) -> AsyncIterator[Record]:
        async def _g(): yield Record(payload={"id": 1})
        return _g()
    async def teardown(self): pass


class _Snk(AbstractSink):
    _table = "stub_table"
    async def setup(self): pass
    async def write(self, records): pass
    async def commit_bookmark(self, b): pass
    async def teardown(self): pass


def _pipe(pid: str = "test-pipe") -> Pipe:
    return Pipe(id=pid, source=_Src(), sink=_Snk(), mode=CopyMode.FULL)


# ---------------------------------------------------------------------------
# ResourceProfile limits
# ---------------------------------------------------------------------------


def test_resource_limits_all_profiles() -> None:
    for profile in ResourceProfile:
        limits = RESOURCE_LIMITS[profile]
        assert "cpu_request" in limits
        assert "memory_request" in limits
        assert "cpu_limit" in limits
        assert "memory_limit" in limits


# ---------------------------------------------------------------------------
# K8sManifestBuilder
# ---------------------------------------------------------------------------


def test_deployment_manifest_structure() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    manifest = builder.deployment("my-pipe", "image:latest", {"KEY": "val"})
    assert manifest["kind"] == "Deployment"
    assert manifest["metadata"]["namespace"] == "staging"
    containers = manifest["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    assert containers[0]["image"] == "image:latest"
    env_names = {e["name"] for e in containers[0]["env"]}
    assert "KEY" in env_names


def test_deployment_resource_profile_applied() -> None:
    builder = K8sManifestBuilder()
    heavy = builder.deployment("x", "img", {}, profile=ResourceProfile.HEAVY)
    c = heavy["spec"]["template"]["spec"]["containers"][0]
    assert c["resources"]["requests"]["cpu"] == RESOURCE_LIMITS[ResourceProfile.HEAVY]["cpu_request"]


def test_configmap_manifest() -> None:
    builder = K8sManifestBuilder()
    cm = builder.configmap("my-config", {"pipe_id": "orders"})
    assert cm["kind"] == "ConfigMap"
    assert cm["data"]["pipe_id"] == "orders"


def test_service_monitor_manifest() -> None:
    builder = K8sManifestBuilder()
    sm = builder.service_monitor("my-pipe")
    assert sm["kind"] == "ServiceMonitor"
    assert sm["spec"]["endpoints"][0]["port"] == "metrics"


def test_keda_scaled_object_manifest() -> None:
    builder = K8sManifestBuilder()
    keda = builder.keda_scaled_object("my-keda", "my-processor", queue_name="onpipes.orders")
    assert keda["kind"] == "ScaledObject"
    assert keda["spec"]["scaleTargetRef"]["name"] == "my-processor"


def test_cronjob_manifest() -> None:
    builder = K8sManifestBuilder()
    cj = builder.cronjob("nightly", "0 2 * * *", "img", {})
    assert cj["kind"] == "CronJob"
    assert cj["spec"]["schedule"] == "0 2 * * *"


# ---------------------------------------------------------------------------
# ManifestGenerator — pipe
# ---------------------------------------------------------------------------


def test_manifest_generator_for_pipe_keys() -> None:
    gen = ManifestGenerator(namespace="prod")
    pipe = _pipe("orders-sync")
    manifests = gen.for_pipe(pipe)
    # Must contain at least a ConfigMap + Deployment + ServiceMonitor
    kinds = {m["kind"] for m in manifests.values()}
    assert "ConfigMap" in kinds
    assert "Deployment" in kinds
    assert "ServiceMonitor" in kinds


def test_manifest_generator_for_pipe_with_schedule_has_cronjob() -> None:
    gen = ManifestGenerator()
    pipe = _pipe("nightly-sync")
    manifests = gen.for_pipe(pipe, schedule="0 2 * * *")
    kinds = {m["kind"] for m in manifests.values()}
    assert "CronJob" in kinds
    assert "Deployment" not in kinds


def test_manifest_generator_for_pipe_configmap_has_pipe_id() -> None:
    gen = ManifestGenerator()
    pipe = _pipe("my-pipe")
    manifests = gen.for_pipe(pipe)
    cm = next(m for m in manifests.values() if m["kind"] == "ConfigMap")
    assert cm["data"]["pipe_id"] == "my-pipe"


# ---------------------------------------------------------------------------
# ManifestGenerator — opus
# ---------------------------------------------------------------------------


def test_manifest_generator_for_opus_keys() -> None:
    opus = Opus(id="nightly-etl", schedule="0 3 * * *")
    opus.add_segments([
        PipeSegment(id="extract", pipe=_pipe("ext")),
        PipeSegment(id="load", pipe=_pipe("load"), depends_on=["extract"]),
    ])
    gen = ManifestGenerator()
    manifests = gen.for_opus(opus)
    kinds = {m["kind"] for m in manifests.values()}
    assert "Deployment" in kinds
    assert "ConfigMap" in kinds
    assert "ScaledObject" in kinds
    assert "CronJob" in kinds  # schedule is set


def test_manifest_generator_for_opus_configmap_has_segment_ids() -> None:
    opus = Opus(id="o")
    opus.add_segments([
        PipeSegment(id="seg-a", pipe=_pipe()),
        PipeSegment(id="seg-b", pipe=_pipe()),
    ])
    gen = ManifestGenerator()
    manifests = gen.for_opus(opus)
    cm = next(m for m in manifests.values() if m["kind"] == "ConfigMap")
    seg_ids = set(cm["data"]["segment_ids"].split(","))
    assert "seg-a" in seg_ids
    assert "seg-b" in seg_ids


# ---------------------------------------------------------------------------
# ManifestGenerator — YAML output
# ---------------------------------------------------------------------------


def test_to_yaml_stream_parseable() -> None:
    import yaml
    gen = ManifestGenerator()
    pipe = _pipe("orders")
    manifests = gen.for_pipe(pipe)
    stream = gen.to_yaml_stream(manifests)
    # Should parse as multiple YAML documents
    docs = list(yaml.safe_load_all(stream))
    assert len(docs) >= 2  # at least ConfigMap + Deployment


# ---------------------------------------------------------------------------
# DockerComposeGenerator
# ---------------------------------------------------------------------------


def test_docker_compose_includes_all_services_by_default() -> None:
    gen = DockerComposeGenerator()
    compose = gen.generate()
    services = set(compose["services"].keys())
    assert "sqlserver" in services
    assert "postgres" in services
    assert "redis" in services
    assert "rabbitmq" in services
    assert "onpipes-runner" in services


def test_docker_compose_exclude_services() -> None:
    gen = DockerComposeGenerator()
    compose = gen.generate(include_sqlserver=False, include_postgres=False, include_runner=False)
    services = set(compose["services"].keys())
    assert "sqlserver" not in services
    assert "postgres" not in services
    assert "onpipes-runner" not in services
    assert "redis" in services


def test_docker_compose_to_yaml_parseable() -> None:
    import yaml
    gen = DockerComposeGenerator()
    compose = gen.generate()
    text = gen.to_yaml(compose)
    parsed = yaml.safe_load(text)
    assert "services" in parsed


def test_docker_compose_volumes_populated_when_services_included() -> None:
    gen = DockerComposeGenerator()
    compose = gen.generate()
    assert "volumes" in compose
    assert "redis-data" in compose["volumes"]
