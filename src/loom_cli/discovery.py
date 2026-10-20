"""Discovery layer — dataclass + union(builtin, catalog, remote)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

Source = Literal["builtin", "catalog", "remote"]
Status = Literal["installed", "available", "remote-only"]


@dataclass(frozen=True)
class DatasetEntry:
    slug: str
    source: Source
    display_name: str
    license_spdx: str
    license_url: str
    task_count: int | None
    status: Status
    available_pip_spec: str | None
    entry_point: str | None
    # Where the benchmark's TASKS come from. Distinct from `source`
    # (which is where THIS DISCOVERY ROW came from). Values:
    #   "huggingface" / "git" / "https-tarball" — entry-point adapter
    #     fetching from a real upstream
    #   "local-folder" — [[local]] in benchmarks.toml (issue #234 PR-1)
    #   None — catalog entries, or remote rows with no upstream info
    upstream_kind: str | None = None


def union_entries(
    *,
    builtin: Iterable[DatasetEntry],
    catalog: Iterable[DatasetEntry],
    remote: Iterable[DatasetEntry],
) -> list[DatasetEntry]:
    """Merge the three sources, deduping on slug.

    Precedence: builtin > remote > catalog. Rationale:
    - If pip-installed locally, that's the source of truth.
    - Else, if a CP service has it, prefer the live service over a
      potentially-stale catalog snapshot.
    - Else, surface as "available" from the catalog.
    """
    seen: dict[str, DatasetEntry] = {}
    for e in catalog:
        seen[e.slug] = e
    for e in remote:
        seen[e.slug] = e
    for e in builtin:
        seen[e.slug] = e
    return sorted(seen.values(), key=lambda x: x.slug)
