"""Docker Compose generator for local development environments."""
from __future__ import annotations

from typing import Any


class DockerComposeGenerator:
    """Generates a ``docker-compose.yml`` structure for local development.

    Includes standard infrastructure services (SQL Server, PostgreSQL, Redis,
    RabbitMQ) and an optional ``onpipes-runner`` service that mounts the
    source tree for hot-reload development.

    Args:
        image_prefix: Registry prefix for openneuronic images, e.g.
            ``"ghcr.io/openneuronic"``.
        compose_version: Docker Compose file format version string.
    """

    def __init__(
        self,
        image_prefix: str = "openneuronic",
        compose_version: str = "3.9",
    ) -> None:
        self._image_prefix = image_prefix
        self._compose_version = compose_version

    def generate(
        self,
        *,
        include_sqlserver: bool = True,
        include_postgres: bool = True,
        include_redis: bool = True,
        include_rabbitmq: bool = True,
        include_runner: bool = True,
        extra_env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return a docker-compose structure as a plain Python dict.

        Args:
            include_sqlserver: Add a SQL Server 2022 service.
            include_postgres: Add a PostgreSQL 16 service.
            include_redis: Add a Redis 7 service.
            include_rabbitmq: Add a RabbitMQ 3 service with management UI.
            include_runner: Add the openneuronic-pipes runner service.
            extra_env: Additional environment variables injected into the
                runner service.
        """
        services: dict[str, Any] = {}
        networks: dict[str, Any] = {"onpipes": {"driver": "bridge"}}
        volumes: dict[str, Any] = {}

        if include_sqlserver:
            volumes["sqlserver-data"] = {}
            services["sqlserver"] = {
                "image": "mcr.microsoft.com/mssql/server:2022-latest",
                "environment": {
                    "ACCEPT_EULA": "Y",
                    "SA_PASSWORD": "YourStrong!Passw0rd",
                    "MSSQL_PID": "Developer",
                },
                "ports": ["1433:1433"],
                "volumes": ["sqlserver-data:/var/opt/mssql"],
                "networks": ["onpipes"],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "/opt/mssql-tools/bin/sqlcmd",
                        "-S", "localhost",
                        "-U", "sa",
                        "-P", "YourStrong!Passw0rd",
                        "-Q", "SELECT 1",
                    ],
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 5,
                },
            }

        if include_postgres:
            volumes["postgres-data"] = {}
            services["postgres"] = {
                "image": "postgres:16-alpine",
                "environment": {
                    "POSTGRES_USER": "onpipes",
                    "POSTGRES_PASSWORD": "onpipes",
                    "POSTGRES_DB": "onpipes",
                },
                "ports": ["5432:5432"],
                "volumes": ["postgres-data:/var/lib/postgresql/data"],
                "networks": ["onpipes"],
                "healthcheck": {
                    "test": ["CMD-SHELL", "pg_isready -U onpipes"],
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 5,
                },
            }

        if include_redis:
            volumes["redis-data"] = {}
            services["redis"] = {
                "image": "redis:7-alpine",
                "ports": ["6379:6379"],
                "volumes": ["redis-data:/data"],
                "networks": ["onpipes"],
                "healthcheck": {
                    "test": ["CMD", "redis-cli", "ping"],
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 5,
                },
            }

        if include_rabbitmq:
            volumes["rabbitmq-data"] = {}
            services["rabbitmq"] = {
                "image": "rabbitmq:3-management-alpine",
                "environment": {
                    "RABBITMQ_DEFAULT_USER": "onpipes",
                    "RABBITMQ_DEFAULT_PASS": "onpipes",
                },
                "ports": ["5672:5672", "15672:15672"],
                "volumes": ["rabbitmq-data:/var/lib/rabbitmq"],
                "networks": ["onpipes"],
                "healthcheck": {
                    "test": ["CMD", "rabbitmq-diagnostics", "ping"],
                    "interval": "30s",
                    "timeout": "10s",
                    "retries": 5,
                },
            }

        if include_runner:
            runner_env: dict[str, str] = {
                "ONPIPES_REDIS_URL": "redis://redis:6379/0",
                "ONPIPES_AMQP_URL": "amqp://onpipes:onpipes@rabbitmq/",
                "ONPIPES_SS_SERVER": "sqlserver",
                "ONPIPES_SS_USER": "sa",
                "ONPIPES_SS_PASSWORD": "YourStrong!Passw0rd",
                "ONPIPES_PG_DSN": "postgresql://onpipes:onpipes@postgres/onpipes",
            }
            runner_env.update(extra_env or {})
            depends: list[str] = [
                s for s in ("sqlserver", "postgres", "redis", "rabbitmq")
                if s in services
            ]
            services["onpipes-runner"] = {
                "image": f"{self._image_prefix}/pipes-runner:latest",
                "environment": runner_env,
                "volumes": ["./src:/app/src:ro"],
                "networks": ["onpipes"],
                "depends_on": depends,
            }

        compose: dict[str, Any] = {
            "version": self._compose_version,
            "services": services,
            "networks": networks,
        }
        if volumes:
            compose["volumes"] = volumes

        return compose

    def to_yaml(self, compose: dict[str, Any]) -> str:
        """Serialise *compose* to a YAML string.

        Requires PyYAML (``pip install 'openneuronic-pipes[deploy]'``).
        Falls back to a JSON representation when PyYAML is not installed.
        """
        try:
            import yaml
            return yaml.dump(compose, default_flow_style=False, sort_keys=False)
        except ImportError:
            import json
            return json.dumps(compose, indent=2)
