"""Shared compatibility suite abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod


class CompatSuite(ABC):
    """Base class for a cloud compatibility suite."""

    name: str
    frontend: str

    def export_env(self) -> dict[str, str]:
        return {}

    def seed(self) -> str | None:
        return None

    @abstractmethod
    def run(self) -> int:
        """Run the suite and return the number of failures."""
