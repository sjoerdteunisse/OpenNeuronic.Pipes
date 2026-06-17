"""openneuronic.pipes.api — Flask REST API for OpenNeuronic.Pipes.

Quick start::

    from openneuronic.pipes.api import create_app, Registry

    reg = Registry()
    reg.pipes.register(my_pipe)

    app = create_app(reg)
    app.run(host="127.0.0.1", port=5000)

Or via the CLI entry point::

    ONPIPES_MODULE=myapp.pipes onpipes-api
"""

from openneuronic.pipes.api.app import create_app
from openneuronic.pipes.api.registry import OpusRegistry, PipeRegistry, Registry
from openneuronic.pipes.api.secrets import SecretStore

__all__ = [
    "create_app",
    "Registry",
    "PipeRegistry",
    "OpusRegistry",
    "SecretStore",
]
