"""Lazy Harbor Hub package materialization.

Harbor's package client is catalog-publisher tooling and intentionally stays
outside Loom's routine developer import path. This module imports it only when
the caller explicitly fetches an ``UpstreamSource(kind="harbor-package")``.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom_benchmarks.base import UpstreamSource

MATERIALIZATION_METADATA_FILENAME = "harbor-materialization.json"


class HarborDatasetError(RuntimeError):
    """A Harbor package dataset could not be materialized safely."""


class HarborPublisherDependencyError(HarborDatasetError):
    """The pinned Harbor catalog-publisher package is unavailable."""


@dataclass(frozen=True)
class HarborDatasetMaterialization:
    """Resolved Harbor package digests and their local materialization root."""

    root: Path
    dataset: str
    revision: str
    metadata_version: str
    package_digests: dict[str, str]


async def download_harbor_dataset(
    source: UpstreamSource, output_dir: Path,
) -> HarborDatasetMaterialization:
    """Resolve and export one pinned Harbor package dataset.

    The metadata lookup and download use the same explicit ``name@revision``
    reference. The returned package digests are captured alongside the export
    so a benchmark-specific lock can validate them before conversion.
    """
    if source.kind != "harbor-package":
        raise ValueError(
            "download_harbor_dataset requires UpstreamSource(kind='harbor-package')",
        )
    if not source.revision:
        raise HarborDatasetError("Harbor package sources require a pinned revision")

    package_client = _load_package_dataset_client()
    client = package_client()
    reference = f"{source.locator}@{source.revision}"
    metadata = await client.get_dataset_metadata(reference)
    items = await client.download_dataset(reference, output_dir=output_dir, export=True)
    # Harbor export is flat (<output>/<task-name>); adapters consume the
    # stable <materialization>/tasks/<task-name> layout. Dataset-level files
    # remain at the materialization root, separate from executable tasks.
    tasks_dir = output_dir / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    for item in items:
        downloaded = Path(item.downloaded_path)
        if downloaded.parent != output_dir or not downloaded.is_dir() or downloaded.is_symlink():
            raise HarborDatasetError("Harbor export task must be a directory directly under output_dir")
        target = tasks_dir / downloaded.name
        if target.exists():
            raise HarborDatasetError("Harbor materialization has an existing task; refresh the cache")
        shutil.move(str(downloaded), target)
    return write_materialization_metadata(
        output_dir,
        source=source,
        metadata=metadata,
        items=items,
    )


def write_materialization_metadata(
    output_dir: Path,
    *,
    source: UpstreamSource,
    metadata: Any,
    items: Iterable[Any],
) -> HarborDatasetMaterialization:
    """Persist resolved package identities next to an exported dataset."""
    metadata_name = getattr(metadata, "name", None)
    if metadata_name != source.locator:
        raise HarborDatasetError(
            "Harbor metadata dataset mismatch: "
            f"expected {source.locator!r}, got {metadata_name!r}",
        )
    metadata_version = getattr(metadata, "version", None)
    if not _is_sha256_digest(metadata_version):
        raise HarborDatasetError(
            "Harbor metadata version must be an immutable sha256 digest",
        )

    metadata_digests = _package_digests(
        getattr(metadata, "task_ids", ()),
        source="Harbor metadata",
    )
    item_digests = _package_digests(
        (getattr(item, "id", None) for item in items),
        source="Harbor download",
    )
    if metadata_digests != item_digests:
        raise HarborDatasetError(
            "Harbor download package identities differ from metadata",
        )

    materialization = HarborDatasetMaterialization(
        root=output_dir,
        dataset=source.locator,
        revision=source.revision or "",
        metadata_version=metadata_version,
        package_digests=metadata_digests,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / MATERIALIZATION_METADATA_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": materialization.dataset,
                "revision": materialization.revision,
                "metadata_version": materialization.metadata_version,
                "package_digests": materialization.package_digests,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    return materialization


def _load_package_dataset_client() -> type[Any]:
    try:
        from harbor.registry.client.package import PackageDatasetClient
    except ImportError as exc:
        raise HarborPublisherDependencyError(
            "Harbor package fetching requires the pinned Harbor "
            "catalog-publisher dependency "
            "(harbor @ git+https://github.com/harbor-framework/harbor.git"
            "@527d50deb63a5d279e8c20593c18a2cbc7f61f9e).",
        ) from exc
    return PackageDatasetClient


def _package_digests(task_ids: Iterable[Any], *, source: str) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for task_id in task_ids:
        if task_id is None or not hasattr(task_id, "get_name"):
            raise HarborDatasetError(f"{source} contains a non-package task identity")
        name = task_id.get_name()
        digest = getattr(task_id, "ref", None)
        if not isinstance(name, str) or not _is_sha256_digest(digest):
            raise HarborDatasetError(
                f"{source} must resolve every task to an immutable sha256 package digest",
            )
        if name in resolved:
            raise HarborDatasetError(f"{source} contains duplicate task {name!r}")
        resolved[name] = digest
    return resolved


def _is_sha256_digest(value: object) -> bool:
    if not isinstance(value, str):
        return False
    prefix, _, hex_digest = value.partition(":")
    return prefix == "sha256" and len(hex_digest) == 64 and all(
        char in "0123456789abcdef" for char in hex_digest
    )
