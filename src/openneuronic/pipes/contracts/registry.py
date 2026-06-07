from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openneuronic.pipes.contracts.base import Contract


class ContractRegistry:
    """Registry mapping (contract_class_name, version) → Contract class."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, int], type[Contract]] = {}

    def register(self, contract_cls: type[Contract]) -> None:
        key = (contract_cls.__name__, contract_cls.__contract_version__)
        self._store[key] = contract_cls

    def get(self, name: str, version: int) -> type[Contract]:
        try:
            return self._store[(name, version)]
        except KeyError:
            raise KeyError(
                f"Contract {name!r} version {version} is not registered"
            ) from None

    def all_versions(self, name: str) -> list[int]:
        return sorted(v for (n, v) in self._store if n == name)

    def get_latest(self, name: str) -> type[Contract]:
        """Return the highest-version contract registered under *name*."""
        versions = self.all_versions(name)
        if not versions:
            raise KeyError(f"No contract registered under name {name!r}") from None
        return self.get(name, versions[-1])


contract_registry = ContractRegistry()
