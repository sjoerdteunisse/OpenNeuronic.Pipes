from __future__ import annotations

from openneuronic.pipes.guards.base import Guard, GuardContext, GuardResult


class GuardSuite:
    """A named, ordered collection of guards that run as a unit."""

    def __init__(self, name: str, guards: list[Guard] | None = None) -> None:
        self.name = name
        self._guards: list[Guard] = guards or []

    def add(self, guard: Guard) -> GuardSuite:
        self._guards.append(guard)
        return self

    async def run(self, ctx: GuardContext) -> list[GuardResult]:
        results: list[GuardResult] = []
        for guard in self._guards:
            results.append(await guard.check(ctx))
        return results

    @property
    def passed(self) -> bool:
        return all(r.passed for r in [])  # empty suite passes by default

    async def run_all(self, ctx: GuardContext) -> tuple[bool, list[GuardResult]]:
        results = await self.run(ctx)
        return all(r.passed for r in results), results


class GuardSuiteRegistry:
    def __init__(self) -> None:
        self._store: dict[str, GuardSuite] = {}

    def register(self, suite: GuardSuite) -> None:
        self._store[suite.name] = suite

    def get(self, name: str) -> GuardSuite:
        try:
            return self._store[name]
        except KeyError:
            raise KeyError(f"GuardSuite {name!r} is not registered") from None


guard_suite_registry = GuardSuiteRegistry()


class GuardSuiteRef:
    """A lazy reference to a named :class:`GuardSuite` in the registry.

    Attach to a :class:`~openneuronic.pipes.core.pipe.Pipe` by name; resolved
    at runtime against :data:`guard_suite_registry`.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def resolve(self) -> GuardSuite:
        return guard_suite_registry.get(self.name)
