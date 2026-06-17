"""PipeRegistry and OpusRegistry — simple in-memory registries used by the API server."""
from __future__ import annotations

from typing import Iterator

from openneuronic.pipes.core.pipe import Pipe
from openneuronic.pipes.opus.opus import Opus


class PipeRegistry:
    """Thread-unsafe in-memory registry of :class:`~openneuronic.pipes.core.pipe.Pipe` objects.

    Usage::

        registry = PipeRegistry()
        registry.register(my_pipe)
        # pass to create_app(registry)
    """

    def __init__(self) -> None:
        self._pipes: dict[str, Pipe] = {}

    def register(self, pipe: Pipe) -> None:
        """Register *pipe* keyed by its ``id``."""
        self._pipes[pipe.id] = pipe

    def get(self, pipe_id: str) -> Pipe | None:
        """Return the pipe for *pipe_id*, or ``None``."""
        return self._pipes.get(pipe_id)

    def all(self) -> list[Pipe]:
        """Return all registered pipes."""
        return list(self._pipes.values())

    def __contains__(self, pipe_id: str) -> bool:
        return pipe_id in self._pipes

    def __iter__(self) -> Iterator[Pipe]:
        return iter(self._pipes.values())

    def __len__(self) -> int:
        return len(self._pipes)


class OpusRegistry:
    """Thread-unsafe in-memory registry of :class:`~openneuronic.pipes.opus.opus.Opus` objects."""

    def __init__(self) -> None:
        self._opuses: dict[str, Opus] = {}

    def register(self, opus: Opus) -> None:
        """Register *opus* keyed by its ``id``."""
        self._opuses[opus.id] = opus

    def get(self, opus_id: str) -> Opus | None:
        """Return the opus for *opus_id*, or ``None``."""
        return self._opuses.get(opus_id)

    def all(self) -> list[Opus]:
        """Return all registered opuses."""
        return list(self._opuses.values())

    def __contains__(self, opus_id: str) -> bool:
        return opus_id in self._opuses

    def __iter__(self) -> Iterator[Opus]:
        return iter(self._opuses.values())

    def __len__(self) -> int:
        return len(self._opuses)


class Registry:
    """Composite holder for both pipe and opus registries.

    Pass a single :class:`Registry` instance to :func:`~openneuronic.pipes.api.app.create_app`.

    Example::

        from openneuronic.pipes.api import Registry

        reg = Registry()
        reg.pipes.register(my_pipe)
        reg.opus.register(my_opus)

        app = create_app(reg)
    """

    def __init__(self) -> None:
        self.pipes = PipeRegistry()
        self.opus = OpusRegistry()
