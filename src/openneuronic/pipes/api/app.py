"""Flask app factory and CLI entry point for the OpenNeuronic.Pipes REST API."""
from __future__ import annotations

import collections
import importlib
import os
from typing import Any

try:
    from flask import Flask, jsonify
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Flask is required for the API server. "
        "Install it with: pip install 'openneuronic-pipes[api]'"
    ) from exc

from openneuronic.pipes.api.registry import Registry
from openneuronic.pipes.api.routes.health import health_bp
from openneuronic.pipes.api.routes.lineage import lineage_bp
from openneuronic.pipes.api.routes.opus import opus_bp
from openneuronic.pipes.api.routes.pipes import pipes_bp
from openneuronic.pipes.api.routes.replay import replay_bp
from openneuronic.pipes.api.routes.runs import runs_bp
from openneuronic.pipes.api.routes.secrets import secrets_bp
from openneuronic.pipes.api.secrets import SecretStore
from openneuronic.pipes.core.bookmark import InMemoryBookmarkStore
from openneuronic.pipes.lineage.graph.model import KnowledgeGraph
from openneuronic.pipes.replay.store import InMemoryReplayStore

# Maximum number of run results kept in the in-memory history ring-buffer.
_RUN_HISTORY_MAX = 200


def create_app(
    registry: Registry | None = None,
    config: dict[str, Any] | None = None,
) -> Flask:
    """Create and configure the Flask application.

    Args:
        registry: A :class:`~openneuronic.pipes.api.registry.Registry` that
            holds pre-registered :class:`~openneuronic.pipes.core.pipe.Pipe`
            and :class:`~openneuronic.pipes.opus.opus.Opus` objects.
            When ``None``, an empty registry is used.
        config: Optional dict of Flask configuration overrides.

    Returns:
        A configured :class:`flask.Flask` application.

    Example::

        from openneuronic.pipes.api import create_app, Registry

        reg = Registry()
        reg.pipes.register(my_pipe)
        reg.opus.register(my_opus)

        app = create_app(reg)
        app.run(host="127.0.0.1", port=5000, debug=True)
    """
    app = Flask(__name__)

    if config:
        app.config.update(config)

    if registry is None:
        registry = Registry()

    # Shared in-memory state attached to the app.
    app.extensions["onpipes"] = {
        "registry": registry,
        "bookmark_store": InMemoryBookmarkStore(),
        "replay_store": InMemoryReplayStore(),
        "lineage_graph": KnowledgeGraph(),
        "secret_store": SecretStore(),
        # Ring-buffer: a deque with a max length keeps the last N run results.
        "run_history": collections.deque(maxlen=_RUN_HISTORY_MAX),
    }

    # Register blueprints.
    app.register_blueprint(health_bp)
    app.register_blueprint(pipes_bp)
    app.register_blueprint(opus_bp)
    app.register_blueprint(runs_bp)
    app.register_blueprint(replay_bp)
    app.register_blueprint(lineage_bp)
    app.register_blueprint(secrets_bp)

    # Global JSON error handlers.
    @app.errorhandler(404)
    def not_found(exc: Any) -> tuple:
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(exc: Any) -> tuple:
        return jsonify({"error": "Method not allowed"}), 405

    @app.errorhandler(500)
    def internal_error(exc: Any) -> tuple:
        return jsonify({"error": "Internal server error"}), 500

    return app


def _load_registry_from_module(module_path: str) -> Registry:
    """Import *module_path* and look for a ``registry`` or ``get_registry()`` symbol.

    The module must expose one of:

    * A module-level ``registry`` attribute of type :class:`Registry`.
    * A callable ``get_registry`` that returns a :class:`Registry`.

    Example user module (``myapp/pipes.py``)::

        from openneuronic.pipes.api import Registry

        registry = Registry()
        registry.pipes.register(my_pipe)

    Then start the server with::

        ONPIPES_MODULE=myapp.pipes onpipes-api
    """
    mod = importlib.import_module(module_path)
    if hasattr(mod, "get_registry") and callable(mod.get_registry):
        result = mod.get_registry()
        if not isinstance(result, Registry):
            raise TypeError(
                f"'get_registry()' in {module_path!r} must return a Registry instance"
            )
        return result
    if hasattr(mod, "registry"):
        reg = mod.registry
        if not isinstance(reg, Registry):
            raise TypeError(
                f"'registry' in {module_path!r} must be a Registry instance"
            )
        return reg
    raise AttributeError(
        f"Module {module_path!r} must expose a 'registry' attribute or 'get_registry()' callable"
    )


def main() -> None:
    """Entry point for the ``onpipes-api`` CLI command.

    Environment variables:

    * ``ONPIPES_MODULE`` — Python module path to load the :class:`Registry` from
      (e.g. ``myapp.pipes``).
    * ``ONPIPES_HOST`` — Host to bind to (default: ``127.0.0.1``).
    * ``ONPIPES_PORT`` — Port to listen on (default: ``5000``).
    * ``ONPIPES_DEBUG`` — Set to ``1`` to enable Flask debug mode.
    """
    module_path = os.environ.get("ONPIPES_MODULE")
    host = os.environ.get("ONPIPES_HOST", "127.0.0.1")
    port = int(os.environ.get("ONPIPES_PORT", "5000"))
    debug = os.environ.get("ONPIPES_DEBUG", "0") == "1"

    registry: Registry | None = None
    if module_path:
        registry = _load_registry_from_module(module_path)

    app = create_app(registry)
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
