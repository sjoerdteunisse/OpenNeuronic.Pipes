from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.core.record import Record


@dataclass
class GuardContext:
    """Runtime context passed to every guard check."""

    pipe_id: str
    batch: list[Record] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class GuardResult:
    guard_name: str
    passed: bool
    message: str = ""

    def __bool__(self) -> bool:
        return self.passed


class Guard(ABC):
    """Base class for all guards.

    Guards are synchronous or async checks that run against a batch of records
    or against operational metadata.  A failing guard signals the runner to
    apply the associated failure policy (retry, skip, abort).
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def check(self, ctx: GuardContext) -> GuardResult:
        """Evaluate the guard and return a :class:`GuardResult`."""
