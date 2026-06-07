"""README example tests — Deployment section."""
from __future__ import annotations

import pytest

from openneuronic.pipes import CopyMode, Opus, Pipe, PipeSegment, Record
from openneuronic.pipes.deploy.docker import DockerComposeGenerator
from openneuronic.pipes.deploy.k8s import K8sManifestBuilder, RESOURCE_LIMITS
from openneuronic.pipes.deploy.manifests import ManifestGenerator
from openneuronic.pipes.core.enums import ResourceProfile
from openneuronic.pipes.sinks._base import AbstractSink
from openneuronic.pipes.sources._base import AbstractSource
from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _NullSource(AbstractSource):
    source_id = "null"
    def read(self, bookmark=None) -> AsyncIterator[Record]:
        async def _g(): return
        return _g()
    async def setup(self) -> None: pass
    async def teardown(self) -> None: pass


class _NullSink(AbstractSink):
    def __init__(self) -> None:
        self._table = "t"
        self._auto_migrate = False
    async def setup(self) -> None: pass
    async def write(self, records) -> None: pass
    async def commit_bookmark(self, b) -> None: pass
    async def teardown(self) -> None: pass


def _pipe() -> Pipe:
    return Pipe(id="deploy-test", source=_NullSource(), sink=_NullSink(), mode=CopyMode.INCREMENTAL)


# ---------------------------------------------------------------------------
# Tests — ManifestGenerator
# ---------------------------------------------------------------------------

def test_manifest_generator_for_pipe_returns_manifests() -> None:
    gen = ManifestGenerator(
        namespace="production",
        image_prefix="registry.openneuronic.com/onpipes",
        image_tag="v1.2.0",
    )
    manifests = gen.for_pipe(_pipe(), profile=ResourceProfile.STANDARD, extra_env={"LOG_LEVEL": "INFO"})
    assert len(manifests) > 0


def test_manifest_generator_for_pipe_with_schedule() -> None:
    gen = ManifestGenerator(
        namespace="production",
        image_prefix="registry.openneuronic.com/onpipes",
        image_tag="v1.2.0",
    )
    manifests = gen.for_pipe(_pipe(), schedule="0 2 * * *")
    assert len(manifests) > 0


def test_manifest_generator_for_opus_returns_manifests() -> None:
    opus = Opus(id="nightly-etl")
    opus.add_segments([PipeSegment(id="step", pipe=_pipe())])

    gen = ManifestGenerator(
        namespace="production",
        image_prefix="registry.openneuronic.com/onpipes",
        image_tag="v1.2.0",
    )
    manifests = gen.for_opus(opus, profile=ResourceProfile.HEAVY)
    assert len(manifests) > 0


def test_manifest_generator_to_yaml_stream() -> None:
    gen = ManifestGenerator(
        namespace="production",
        image_prefix="registry.openneuronic.com/onpipes",
        image_tag="v1.0.0",
    )
    manifests = gen.for_pipe(_pipe())
    yaml_stream = gen.to_yaml_stream(manifests)
    assert isinstance(yaml_stream, str)
    assert len(yaml_stream) > 0


# ---------------------------------------------------------------------------
# Tests — K8sManifestBuilder
# ---------------------------------------------------------------------------

def test_k8s_builder_deployment_contains_name() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    deploy = builder.deployment("orders-runner", "registry/onpipes:latest", {"ENV": "staging"})
    assert deploy is not None


def test_k8s_builder_configmap_contains_data() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    cm = builder.configmap("orders-config", {"pipe_id": "orders-sync"})
    assert cm is not None


def test_k8s_builder_service_monitor() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    sm = builder.service_monitor("orders-runner")
    assert sm is not None


def test_k8s_builder_keda_scaled_object() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    keda = builder.keda_scaled_object("orders-keda", "orders-processor", queue_name="orders.processed")
    assert keda is not None


def test_k8s_builder_cronjob() -> None:
    builder = K8sManifestBuilder(namespace="staging")
    cj = builder.cronjob("orders-nightly", "0 2 * * *", "registry/onpipes:latest", {})
    assert cj is not None


def test_resource_limits_heavy_profile() -> None:
    limits = RESOURCE_LIMITS[ResourceProfile.HEAVY]
    assert "cpu_limit" in limits
    assert "memory_limit" in limits


# ---------------------------------------------------------------------------
# Tests — DockerComposeGenerator
# ---------------------------------------------------------------------------

def test_docker_compose_generator_produces_yaml() -> None:
    gen = DockerComposeGenerator()
    compose = gen.generate(
        include_sqlserver=True,
        include_postgres=True,
        include_redis=True,
        include_rabbitmq=True,
        include_runner=True,
    )
    yaml_text = gen.to_yaml(compose)
    assert isinstance(yaml_text, str)
    assert len(yaml_text) > 0
