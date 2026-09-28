"""Shared publication input types; importing these does not load adapters."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AdapterBenchmarkEntry:
    id: str
    display_name: str
    series: str | None
    license_spdx: str


@dataclass(frozen=True)
class PreparedAdapterBenchmark:
    entry: AdapterBenchmarkEntry
    task_root: Path
    task_tomls: tuple[Path, ...]
    manifest: dict[str, Any]
    tasks: dict[str, dict[str, Any]]
    warnings: tuple[str, ...]

    @property
    def task_count(self) -> int:
        return len(self.task_tomls)
