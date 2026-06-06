"""ManifestGenerator — produce K8s and Docker Compose manifests for pipes/opuses."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from openneuronic.pipes.core.enums import ResourceProfile
from openneuronic.pipes.deploy.k8s import K8sManifestBuilder

if TYPE_CHECKING:
    from openneuronic.pipes.core.pipe import Pipe
    from openneuronic.pipes.opus.opus import Opus


class ManifestGenerator:
    """Generates Kubernetes and Docker Compose manifests.

    Args:
        namespace: Kubernetes namespace for all resources.
        image_prefix: Container registry prefix, e.g. ``"ghcr.io/org"``.
        image_tag: Docker image tag to embed in manifests.
    """

    def __init__(
        self,
        namespace: str = "default",
        image_prefix: str = "openneuronic",
        image_tag: str = "latest",
    ) -> None:
        self._ns = namespace
        self._image_prefix = image_prefix
        self._image_tag = image_tag
        self._k8s = K8sManifestBuilder(namespace=namespace)

    # ------------------------------------------------------------------
    # Pipe manifests
    # ------------------------------------------------------------------

    def for_pipe(
        self,
        pipe: Pipe,
        profile: ResourceProfile = ResourceProfile.STANDARD,
        schedule: str | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Generate K8s manifests for a single *pipe*.

        Returns a dict of resource name → manifest dict.  The caller can
        iterate and serialise each manifest to YAML.

        Produces:
        - A **ConfigMap** with pipe metadata.
        - A **Deployment** for the local-runner image (or a **CronJob** when
          *schedule* is provided).
        - A **ServiceMonitor** for Prometheus scraping.
        """
        manifests: dict[str, Any] = {}
        name = f"onpipes-pipe-{pipe.id.lower().replace('_', '-')}"
        image = f"{self._image_prefix}/pipes-runner:{self._image_tag}"

        env: dict[str, str] = {
            "ONPIPES_PIPE_ID": pipe.id,
            "ONPIPES_MODE": str(pipe.mode),
            "ONPIPES_SOURCE": type(pipe.source).__name__,
            "ONPIPES_SINK": type(pipe.sink).__name__,
        }
        env.update(extra_env or {})

        # ConfigMap
        manifests[f"{name}-config"] = self._k8s.configmap(
            name=f"{name}-config",
            data={
                "pipe_id": pipe.id,
                "mode": str(pipe.mode),
                "source_type": type(pipe.source).__name__,
                "sink_type": type(pipe.sink).__name__,
                "schema": getattr(pipe.schema, "__name__", "") if pipe.schema else "",
            },
        )

        if schedule:
            manifests[f"{name}-cronjob"] = self._k8s.cronjob(
                name=f"{name}-cronjob",
                schedule=schedule,
                image=image,
                env=env,
                profile=profile,
            )
        else:
            manifests[f"{name}-deployment"] = self._k8s.deployment(
                name=name,
                image=image,
                env=env,
                profile=profile,
            )

        # ServiceMonitor
        manifests[f"{name}-monitor"] = self._k8s.service_monitor(name=name)

        return manifests

    # ------------------------------------------------------------------
    # Opus manifests
    # ------------------------------------------------------------------

    def for_opus(
        self,
        opus: Opus,
        profile: ResourceProfile = ResourceProfile.STANDARD,
        extra_env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Generate K8s manifests for an *opus* and all its segments.

        Produces a set of manifests per segment plus an orchestrator Deployment
        and a KEDA :class:`ScaledObject` for the processor tier.
        """
        manifests: dict[str, Any] = {}
        opus_name = f"onpipes-opus-{opus.id.lower().replace('_', '-')}"
        orchestrator_image = f"{self._image_prefix}/pipes-orchestrator:{self._image_tag}"

        env: dict[str, str] = {"ONPIPES_OPUS_ID": opus.id}
        env.update(extra_env or {})

        # Orchestrator Deployment
        manifests[f"{opus_name}-orchestrator"] = self._k8s.deployment(
            name=f"{opus_name}-orchestrator",
            image=orchestrator_image,
            env=env,
            profile=profile,
            replicas=1,
        )

        # ConfigMap with segment IDs
        manifests[f"{opus_name}-config"] = self._k8s.configmap(
            name=f"{opus_name}-config",
            data={
                "opus_id": opus.id,
                "segment_ids": ",".join(s.id for s in opus.segments),
                "schedule": opus.schedule or "",
                "on_failure": str(opus.on_failure),
                "durable": str(opus.durable),
            },
        )

        # KEDA ScaledObject for processor workers
        processor_image = f"{self._image_prefix}/pipes-processor:{self._image_tag}"
        processor_name = f"{opus_name}-processor"
        manifests[processor_name] = self._k8s.deployment(
            name=processor_name,
            image=processor_image,
            env=env,
            profile=profile,
            replicas=1,
        )
        manifests[f"{processor_name}-keda"] = self._k8s.keda_scaled_object(
            name=f"{processor_name}-keda",
            deployment_name=processor_name,
            queue_name=f"onpipes.{opus.id}",
        )

        # Optional CronJob for scheduled opuses
        if opus.schedule:
            manifests[f"{opus_name}-cronjob"] = self._k8s.cronjob(
                name=f"{opus_name}-cronjob",
                schedule=opus.schedule,
                image=orchestrator_image,
                env=env,
                profile=profile,
            )

        return manifests

    # ------------------------------------------------------------------
    # YAML serialisation
    # ------------------------------------------------------------------

    def to_yaml_stream(self, manifests: dict[str, Any]) -> str:
        """Serialise all *manifests* as a multi-document YAML string.

        Requires PyYAML (``pip install 'openneuronic-pipes[deploy]'``).
        """
        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "pyyaml is required for YAML output. "
                "Install with: pip install 'openneuronic-pipes[deploy]'"
            ) from exc

        docs = [
            yaml.dump(manifest, default_flow_style=False, sort_keys=False)
            for manifest in manifests.values()
        ]
        return "---\n" + "\n---\n".join(docs)
