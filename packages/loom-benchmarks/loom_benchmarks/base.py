"""Adapter contract + value types (benchmark integrations spec §3).

Each benchmark adapter declares its upstream source + license up-front
(class attributes) and implements two pure operations:

- `list_instances` walks a fetched source dir and yields
  `BenchmarkInstance` records.
- `convert_instance` writes the canonical Loom task layout (task.toml +
  instruction.md + solution/ + tests/ [+ environment/]) into `out_dir`
  and returns a `ConvertedTask` carrying the new task_id, the
  content-hash checksum, the license SPDX tag, and any warnings the
  conversion emitted.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Literal, Protocol, runtime_checkable


@dataclass(frozen=True)
class UpstreamSource:
    kind: Literal["huggingface", "git", "https-tarball", "harbor-package"]
    locator: str
    revision: str | None = None
    subset: str | None = None
    # Legacy HF script-source marker, retained for catalog identity. Only the
    # reviewed pinned MATH/LiveCodeBench data layouts are supported; no remote
    # Python is executed. Other script sources fail with a migration diagnostic.
    trust_remote_code: bool = False


@dataclass(frozen=True)
class BenchmarkInstance:
    instance_id: str
    split: str
    raw: dict[str, Any]
    # Open-ended key→value metadata surfaced to the SPA's tag filter
    # and the per-task `tags` column. Adapter convention: lowercase
    # string keys, stringified values (year="2024", exam="I",
    # verified="true"). Default empty so legacy adapters keep working.
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ConvertedTask:
    task_id: str
    checksum: str
    license_spdx: str
    warnings: tuple[str, ...]


@runtime_checkable
class BenchmarkAdapter(Protocol):
    name: str
    display_name: str
    upstream_source: UpstreamSource
    license_spdx: str
    license_url: str
    splits: tuple[str, ...]

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Yield BenchmarkInstance records. Pure: same source_dir +
        split → same order."""
        ...

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        """Write task.toml + instruction.md (+ solution/, tests/,
        environment/) into out_dir."""
        ...


class CatalogBackedAdapter:
    """Mixin: pull adapter metadata from `loom_benchmarks/benchmarks.json`
    keyed by `cls.name`.

    Subclasses declare just `name` (the catalog key) + their
    `list_instances` / `convert_instance` methods. The mixin installs
    `display_name`, `series`, `upstream_source`, `license_spdx`,
    `license_url`, `splits` from the JSON entry at class-creation
    time, and exposes the per-entry `params` dict as `cls._params` so
    parametric adapters (e.g. AIME's per-year siblings) can read
    `self._params["year"]` instead of repeating the value as a class
    attr.

    Falls back to the legacy class-attr pattern when the catalog has
    no entry for `cls.name` — third-party adapter packages don't need
    to ship a benchmarks.json to keep working.

    Abstract base classes (the ones that don't pick a `name` yet) are
    skipped: `__init_subclass__` only fires the catalog lookup when
    the subclass has a non-empty `name` attribute set on itself
    (i.e. not inherited unset)."""

    name: ClassVar[str] = ""
    display_name: ClassVar[str] = ""
    series: ClassVar[str | None] = None
    upstream_source: ClassVar[UpstreamSource]
    license_spdx: ClassVar[str] = ""
    license_url: ClassVar[str] = ""
    splits: ClassVar[tuple[str, ...]] = ()
    _params: ClassVar[dict[str, str]] = {}
    license_execution_policy: ClassVar[str] = "allowlist"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("name")
        if not isinstance(name, str) or not name:
            return
        # Lazy import to avoid a base.py ↔ catalog.py cycle at module
        # load: catalog.py imports UpstreamSource from base.py.
        from loom_benchmarks.catalog import CATALOG

        entry = CATALOG.get(name)
        if entry is None:
            return
        cls.display_name = entry.display_name
        if entry.series is not None:
            cls.series = entry.series
        cls.upstream_source = entry.upstream.to_dataclass()
        cls.license_spdx = entry.license.spdx
        cls.license_url = entry.license.url
        cls.license_execution_policy = entry.license.execution_policy
        cls.splits = tuple(entry.splits)
        cls._params = dict(entry.params)
