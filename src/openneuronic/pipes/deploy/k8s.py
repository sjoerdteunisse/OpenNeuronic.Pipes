"""Kubernetes manifest builder for OpenNeuronic.Pipes deployments."""
from __future__ import annotations

from typing import Any

from openneuronic.pipes.core.enums import ResourceProfile


# ---------------------------------------------------------------------------
# Resource profiles → K8s CPU / memory limits
# ---------------------------------------------------------------------------

RESOURCE_LIMITS: dict[ResourceProfile, dict[str, str]] = {
    ResourceProfile.LIGHT: {
        "cpu_request": "100m",
        "cpu_limit": "500m",
        "memory_request": "128Mi",
        "memory_limit": "512Mi",
    },
    ResourceProfile.STANDARD: {
        "cpu_request": "500m",
        "cpu_limit": "1000m",
        "memory_request": "512Mi",
        "memory_limit": "2Gi",
    },
    ResourceProfile.HEAVY: {
        "cpu_request": "1000m",
        "cpu_limit": "4000m",
        "memory_request": "2Gi",
        "memory_limit": "8Gi",
    },
}


class K8sManifestBuilder:
    """Builds individual Kubernetes resource manifests as plain Python dicts.

    Dicts can be serialised to YAML by :class:`~openneuronic.pipes.deploy.manifests.ManifestGenerator`.
    """

    def __init__(self, namespace: str = "default") -> None:
        self._ns = namespace

    def deployment(
        self,
        name: str,
        image: str,
        env: dict[str, str],
        profile: ResourceProfile = ResourceProfile.STANDARD,
        replicas: int = 1,
        labels: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        lbl = {"app": name, **(labels or {})}
        res = RESOURCE_LIMITS[profile]
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": self._ns, "labels": lbl},
            "spec": {
                "replicas": replicas,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": lbl},
                    "spec": {
                        "containers": [
                            {
                                "name": name,
                                "image": image,
                                "env": [
                                    {"name": k, "value": v}
                                    for k, v in env.items()
                                ],
                                "resources": {
                                    "requests": {
                                        "cpu": res["cpu_request"],
                                        "memory": res["memory_request"],
                                    },
                                    "limits": {
                                        "cpu": res["cpu_limit"],
                                        "memory": res["memory_limit"],
                                    },
                                },
                            }
                        ]
                    },
                },
            },
        }

    def configmap(
        self,
        name: str,
        data: dict[str, str],
    ) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": self._ns},
            "data": data,
        }

    def service_monitor(
        self,
        name: str,
        port: str = "metrics",
        interval: str = "30s",
    ) -> dict[str, Any]:
        return {
            "apiVersion": "monitoring.coreos.com/v1",
            "kind": "ServiceMonitor",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": {
                "selector": {"matchLabels": {"app": name}},
                "endpoints": [{"port": port, "interval": interval}],
            },
        }

    def keda_scaled_object(
        self,
        name: str,
        deployment_name: str,
        queue_host: str = "rabbitmq",
        queue_name: str = "",
        queue_length: int = 100,
        min_replicas: int = 0,
        max_replicas: int = 10,
    ) -> dict[str, Any]:
        return {
            "apiVersion": "keda.sh/v1alpha1",
            "kind": "ScaledObject",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": {
                "scaleTargetRef": {"name": deployment_name},
                "minReplicaCount": min_replicas,
                "maxReplicaCount": max_replicas,
                "triggers": [
                    {
                        "type": "rabbitmq",
                        "metadata": {
                            "host": queue_host,
                            "queueName": queue_name or deployment_name,
                            "queueLength": str(queue_length),
                        },
                    }
                ],
            },
        }

    def cronjob(
        self,
        name: str,
        schedule: str,
        image: str,
        env: dict[str, str],
        profile: ResourceProfile = ResourceProfile.STANDARD,
    ) -> dict[str, Any]:
        res = RESOURCE_LIMITS[profile]
        return {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": {
                "schedule": schedule,
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": name,
                                        "image": image,
                                        "env": [
                                            {"name": k, "value": v}
                                            for k, v in env.items()
                                        ],
                                        "resources": {
                                            "requests": {
                                                "cpu": res["cpu_request"],
                                                "memory": res["memory_request"],
                                            },
                                            "limits": {
                                                "cpu": res["cpu_limit"],
                                                "memory": res["memory_limit"],
                                            },
                                        },
                                    }
                                ],
                                "restartPolicy": "OnFailure",
                            }
                        }
                    }
                },
            },
        }
